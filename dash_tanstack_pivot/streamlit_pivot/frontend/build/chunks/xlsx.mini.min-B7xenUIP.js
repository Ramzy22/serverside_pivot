import { g as Bf } from "./charts-DthW1Q8W.js";
function Hf(xt, or) {
  for (var Wt = 0; Wt < or.length; Wt++) {
    const dt = or[Wt];
    if (typeof dt != "string" && !Array.isArray(dt)) {
      for (const le in dt)
        if (le !== "default" && !(le in xt)) {
          const ft = Object.getOwnPropertyDescriptor(dt, le);
          ft && Object.defineProperty(xt, le, ft.get ? ft : {
            enumerable: !0,
            get: () => dt[le]
          });
        }
    }
  }
  return Object.freeze(Object.defineProperty(xt, Symbol.toStringTag, { value: "Module" }));
}
function Ns(xt) {
  throw new Error('Could not dynamically require "' + xt + '". Please configure the dynamicRequireTargets or/and ignoreDynamicRequires option of @rollup/plugin-commonjs appropriately for this require call to work.');
}
var Or = { exports: {} };
/*! xlsx.js (C) 2013-present SheetJS -- http://sheetjs.com */
var Ps;
function Vf() {
  return Ps || (Ps = 1, (function(xt, or) {
    var Wt = {};
    function dt(le) {
      le.version = "0.18.5";
      var ft = 1200, It = 1252, Is = [874, 932, 936, 949, 950, 1250, 1251, 1252, 1253, 1254, 1255, 1256, 1257, 1258, 1e4], Mr = { 0: 1252, 1: 65001, 2: 65001, 77: 1e4, 128: 932, 129: 949, 130: 1361, 134: 936, 136: 950, 161: 1253, 162: 1254, 163: 1258, 177: 1255, 178: 1256, 186: 1257, 204: 1251, 222: 874, 238: 1250, 255: 1252, 69: 6969 }, Fa = function(e) {
        Is.indexOf(e) != -1 && (It = Mr[0] = e);
      };
      function Ls() {
        Fa(1252);
      }
      var Rr = function(e) {
        ft = e, Fa(e);
      };
      function Nr() {
        Rr(1200), Ls();
      }
      function Ta(e) {
        for (var r = [], t = 0, n = e.length; t < n; ++t) r[t] = e.charCodeAt(t);
        return r;
      }
      function $s(e) {
        for (var r = [], t = 0; t < e.length >> 1; ++t) r[t] = String.fromCharCode(e.charCodeAt(2 * t) + (e.charCodeAt(2 * t + 1) << 8));
        return r.join("");
      }
      function zs(e) {
        for (var r = [], t = 0; t < e.length >> 1; ++t) r[t] = String.fromCharCode(e.charCodeAt(2 * t + 1) + (e.charCodeAt(2 * t) << 8));
        return r.join("");
      }
      var lr = function(e) {
        var r = e.charCodeAt(0), t = e.charCodeAt(1);
        return r == 255 && t == 254 ? $s(e.slice(2)) : r == 254 && t == 255 ? zs(e.slice(2)) : r == 65279 ? e.slice(1) : e;
      }, cr = function(r) {
        return String.fromCharCode(r);
      }, Ea = function(r) {
        return String.fromCharCode(r);
      }, ut = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=";
      function Lt(e) {
        for (var r = "", t = 0, n = 0, a = 0, s = 0, i = 0, o = 0, c = 0, f = 0; f < e.length; )
          t = e.charCodeAt(f++), s = t >> 2, n = e.charCodeAt(f++), i = (t & 3) << 4 | n >> 4, a = e.charCodeAt(f++), o = (n & 15) << 2 | a >> 6, c = a & 63, isNaN(n) ? o = c = 64 : isNaN(a) && (c = 64), r += ut.charAt(s) + ut.charAt(i) + ut.charAt(o) + ut.charAt(c);
        return r;
      }
      function Qe(e) {
        var r = "", t = 0, n = 0, a = 0, s = 0, i = 0, o = 0, c = 0;
        e = e.replace(/[^\w\+\/\=]/g, "");
        for (var f = 0; f < e.length; )
          s = ut.indexOf(e.charAt(f++)), i = ut.indexOf(e.charAt(f++)), t = s << 2 | i >> 4, r += String.fromCharCode(t), o = ut.indexOf(e.charAt(f++)), n = (i & 15) << 4 | o >> 2, o !== 64 && (r += String.fromCharCode(n)), c = ut.indexOf(e.charAt(f++)), a = (o & 3) << 6 | c, c !== 64 && (r += String.fromCharCode(a));
        return r;
      }
      var Pr = /* @__PURE__ */ (function() {
        return typeof Buffer < "u" && !1;
      })();
      (function() {
        if (typeof Buffer < "u") {
          var e = !Buffer.from;
          if (!e) try {
            Buffer.from("foo", "utf8");
          } catch {
            e = !0;
          }
          return e ? function(r, t) {
            return t ? new Buffer(r, t) : new Buffer(r);
          } : Buffer.from.bind(Buffer);
        }
        return function() {
        };
      })();
      function $t(e) {
        return typeof Uint8Array < "u" ? new Uint8Array(e) : new Array(e);
      }
      function js(e) {
        return typeof Uint8Array < "u" ? new Uint8Array(e) : new Array(e);
      }
      var ht = function(r) {
        return r.split("").map(function(t) {
          return t.charCodeAt(0) & 255;
        });
      };
      function zt(e) {
        if (typeof ArrayBuffer > "u") return ht(e);
        for (var r = new ArrayBuffer(e.length), t = new Uint8Array(r), n = 0; n != e.length; ++n) t[n] = e.charCodeAt(n) & 255;
        return r;
      }
      function jt(e) {
        if (Array.isArray(e)) return e.map(function(n) {
          return String.fromCharCode(n);
        }).join("");
        for (var r = [], t = 0; t < e.length; ++t) r[t] = String.fromCharCode(e[t]);
        return r.join("");
      }
      function Us(e) {
        if (typeof Uint8Array > "u") throw new Error("Unsupported");
        return new Uint8Array(e);
      }
      function Wr(e) {
        if (typeof ArrayBuffer > "u") throw new Error("Unsupported");
        if (e instanceof ArrayBuffer) return Wr(new Uint8Array(e));
        for (var r = new Array(e.length), t = 0; t < e.length; ++t) r[t] = e[t];
        return r;
      }
      var vt = function(e) {
        if (typeof Uint8Array < "u") {
          var r = 0, t = 0;
          for (r = 0; r < e.length; ++r) t += e[r].length;
          var n = new Uint8Array(t), a = 0;
          for (r = 0, t = 0; r < e.length; t += a, ++r)
            if (a = e[r].length, e[r] instanceof Uint8Array) n.set(e[r], t);
            else {
              if (typeof e[r] == "string")
                throw "wtf";
              n.set(new Uint8Array(e[r]), t);
            }
          return n;
        }
        return [].concat.apply([], e.map(function(s) {
          return Array.isArray(s) ? s : [].slice.call(s);
        }));
      };
      function Bs(e) {
        for (var r = [], t = 0, n = e.length + 250, a = $t(e.length + 255), s = 0; s < e.length; ++s) {
          var i = e.charCodeAt(s);
          if (i < 128) a[t++] = i;
          else if (i < 2048)
            a[t++] = 192 | i >> 6 & 31, a[t++] = 128 | i & 63;
          else if (i >= 55296 && i < 57344) {
            i = (i & 1023) + 64;
            var o = e.charCodeAt(++s) & 1023;
            a[t++] = 240 | i >> 8 & 7, a[t++] = 128 | i >> 2 & 63, a[t++] = 128 | o >> 6 & 15 | (i & 3) << 4, a[t++] = 128 | o & 63;
          } else
            a[t++] = 224 | i >> 12 & 15, a[t++] = 128 | i >> 6 & 63, a[t++] = 128 | i & 63;
          t > n && (r.push(a.slice(0, t)), t = 0, a = $t(65535), n = 65530);
        }
        return r.push(a.slice(0, t)), vt(r);
      }
      var Ut = /\u0000/g, fr = /[\u0001-\u0006]/g;
      function St(e) {
        for (var r = "", t = e.length - 1; t >= 0; ) r += e.charAt(t--);
        return r;
      }
      function Be(e, r) {
        var t = "" + e;
        return t.length >= r ? t : Re("0", r - t.length) + t;
      }
      function Ir(e, r) {
        var t = "" + e;
        return t.length >= r ? t : Re(" ", r - t.length) + t;
      }
      function ur(e, r) {
        var t = "" + e;
        return t.length >= r ? t : t + Re(" ", r - t.length);
      }
      function Hs(e, r) {
        var t = "" + Math.round(e);
        return t.length >= r ? t : Re("0", r - t.length) + t;
      }
      function Vs(e, r) {
        var t = "" + e;
        return t.length >= r ? t : Re("0", r - t.length) + t;
      }
      var Da = Math.pow(2, 32);
      function Ct(e, r) {
        if (e > Da || e < -Da) return Hs(e, r);
        var t = Math.round(e);
        return Vs(t, r);
      }
      function hr(e, r) {
        return r = r || 0, e.length >= 7 + r && (e.charCodeAt(r) | 32) === 103 && (e.charCodeAt(r + 1) | 32) === 101 && (e.charCodeAt(r + 2) | 32) === 110 && (e.charCodeAt(r + 3) | 32) === 101 && (e.charCodeAt(r + 4) | 32) === 114 && (e.charCodeAt(r + 5) | 32) === 97 && (e.charCodeAt(r + 6) | 32) === 108;
      }
      var Oa = [["Sun", "Sunday"], ["Mon", "Monday"], ["Tue", "Tuesday"], ["Wed", "Wednesday"], ["Thu", "Thursday"], ["Fri", "Friday"], ["Sat", "Saturday"]], Lr = [["J", "Jan", "January"], ["F", "Feb", "February"], ["M", "Mar", "March"], ["A", "Apr", "April"], ["M", "May", "May"], ["J", "Jun", "June"], ["J", "Jul", "July"], ["A", "Aug", "August"], ["S", "Sep", "September"], ["O", "Oct", "October"], ["N", "Nov", "November"], ["D", "Dec", "December"]];
      function Xs(e) {
        return e || (e = {}), e[0] = "General", e[1] = "0", e[2] = "0.00", e[3] = "#,##0", e[4] = "#,##0.00", e[9] = "0%", e[10] = "0.00%", e[11] = "0.00E+00", e[12] = "# ?/?", e[13] = "# ??/??", e[14] = "m/d/yy", e[15] = "d-mmm-yy", e[16] = "d-mmm", e[17] = "mmm-yy", e[18] = "h:mm AM/PM", e[19] = "h:mm:ss AM/PM", e[20] = "h:mm", e[21] = "h:mm:ss", e[22] = "m/d/yy h:mm", e[37] = "#,##0 ;(#,##0)", e[38] = "#,##0 ;[Red](#,##0)", e[39] = "#,##0.00;(#,##0.00)", e[40] = "#,##0.00;[Red](#,##0.00)", e[45] = "mm:ss", e[46] = "[h]:mm:ss", e[47] = "mmss.0", e[48] = "##0.0E+0", e[49] = "@", e[56] = '"上午/下午 "hh"時"mm"分"ss"秒 "', e;
      }
      var ce = { 0: "General", 1: "0", 2: "0.00", 3: "#,##0", 4: "#,##0.00", 9: "0%", 10: "0.00%", 11: "0.00E+00", 12: "# ?/?", 13: "# ??/??", 14: "m/d/yy", 15: "d-mmm-yy", 16: "d-mmm", 17: "mmm-yy", 18: "h:mm AM/PM", 19: "h:mm:ss AM/PM", 20: "h:mm", 21: "h:mm:ss", 22: "m/d/yy h:mm", 37: "#,##0 ;(#,##0)", 38: "#,##0 ;[Red](#,##0)", 39: "#,##0.00;(#,##0.00)", 40: "#,##0.00;[Red](#,##0.00)", 45: "mm:ss", 46: "[h]:mm:ss", 47: "mmss.0", 48: "##0.0E+0", 49: "@", 56: '"上午/下午 "hh"時"mm"分"ss"秒 "' }, Ma = { 5: 37, 6: 38, 7: 39, 8: 40, 23: 0, 24: 0, 25: 0, 26: 0, 27: 14, 28: 14, 29: 14, 30: 14, 31: 14, 50: 14, 51: 14, 52: 14, 53: 14, 54: 14, 55: 14, 56: 14, 57: 14, 58: 14, 59: 1, 60: 2, 61: 3, 62: 4, 67: 9, 68: 10, 69: 12, 70: 13, 71: 14, 72: 14, 73: 15, 74: 16, 75: 17, 76: 20, 77: 21, 78: 22, 79: 45, 80: 46, 81: 47, 82: 0 }, Gs = { 5: '"$"#,##0_);\\("$"#,##0\\)', 63: '"$"#,##0_);\\("$"#,##0\\)', 6: '"$"#,##0_);[Red]\\("$"#,##0\\)', 64: '"$"#,##0_);[Red]\\("$"#,##0\\)', 7: '"$"#,##0.00_);\\("$"#,##0.00\\)', 65: '"$"#,##0.00_);\\("$"#,##0.00\\)', 8: '"$"#,##0.00_);[Red]\\("$"#,##0.00\\)', 66: '"$"#,##0.00_);[Red]\\("$"#,##0.00\\)', 41: '_(* #,##0_);_(* \\(#,##0\\);_(* "-"_);_(@_)', 42: '_("$"* #,##0_);_("$"* \\(#,##0\\);_("$"* "-"_);_(@_)', 43: '_(* #,##0.00_);_(* \\(#,##0.00\\);_(* "-"??_);_(@_)', 44: '_("$"* #,##0.00_);_("$"* \\(#,##0.00\\);_("$"* "-"??_);_(@_)' };
      function pr(e, r, t) {
        for (var n = e < 0 ? -1 : 1, a = e * n, s = 0, i = 1, o = 0, c = 1, f = 0, l = 0, p = Math.floor(a); f < r && (p = Math.floor(a), o = p * i + s, l = p * f + c, !(a - p < 5e-8)); )
          a = 1 / (a - p), s = i, i = o, c = f, f = l;
        if (l > r && (f > r ? (l = c, o = s) : (l = f, o = i)), !t) return [0, n * o, l];
        var d = Math.floor(n * o / l);
        return [d, n * o - d * l, l];
      }
      function Bt(e, r, t) {
        if (e > 2958465 || e < 0) return null;
        var n = e | 0, a = Math.floor(86400 * (e - n)), s = 0, i = [], o = { D: n, T: a, u: 86400 * (e - n) - a, y: 0, m: 0, d: 0, H: 0, M: 0, S: 0, q: 0 };
        if (Math.abs(o.u) < 1e-6 && (o.u = 0), r && r.date1904 && (n += 1462), o.u > 0.9999 && (o.u = 0, ++a == 86400 && (o.T = a = 0, ++n, ++o.D)), n === 60)
          i = t ? [1317, 10, 29] : [1900, 2, 29], s = 3;
        else if (n === 0)
          i = t ? [1317, 8, 29] : [1900, 1, 0], s = 6;
        else {
          n > 60 && --n;
          var c = new Date(1900, 0, 1);
          c.setDate(c.getDate() + n - 1), i = [c.getFullYear(), c.getMonth() + 1, c.getDate()], s = c.getDay(), n < 60 && (s = (s + 6) % 7), t && (s = Qs(c, i));
        }
        return o.y = i[0], o.m = i[1], o.d = i[2], o.S = a % 60, a = Math.floor(a / 60), o.M = a % 60, a = Math.floor(a / 60), o.H = a, o.q = s, o;
      }
      var Ra = new Date(1899, 11, 31, 0, 0, 0), Ys = Ra.getTime(), qs = new Date(1900, 2, 1, 0, 0, 0);
      function Na(e, r) {
        var t = e.getTime();
        return r ? t -= 1461 * 24 * 60 * 60 * 1e3 : e >= qs && (t += 1440 * 60 * 1e3), (t - (Ys + (e.getTimezoneOffset() - Ra.getTimezoneOffset()) * 6e4)) / (1440 * 60 * 1e3);
      }
      function $r(e) {
        return e.indexOf(".") == -1 ? e : e.replace(/(?:\.0*|(\.\d*[1-9])0+)$/, "$1");
      }
      function Js(e) {
        return e.indexOf("E") == -1 ? e : e.replace(/(?:\.0*|(\.\d*[1-9])0+)[Ee]/, "$1E").replace(/(E[+-])(\d)$/, "$10$2");
      }
      function Ks(e) {
        var r = e < 0 ? 12 : 11, t = $r(e.toFixed(12));
        return t.length <= r || (t = e.toPrecision(10), t.length <= r) ? t : e.toExponential(5);
      }
      function Zs(e) {
        var r = $r(e.toFixed(11));
        return r.length > (e < 0 ? 12 : 11) || r === "0" || r === "-0" ? e.toPrecision(6) : r;
      }
      function zr(e) {
        var r = Math.floor(Math.log(Math.abs(e)) * Math.LOG10E), t;
        return r >= -4 && r <= -1 ? t = e.toPrecision(10 + r) : Math.abs(r) <= 9 ? t = Ks(e) : r === 10 ? t = e.toFixed(10).substr(0, 12) : t = Zs(e), $r(Js(t.toUpperCase()));
      }
      function mr(e, r) {
        switch (typeof e) {
          case "string":
            return e;
          case "boolean":
            return e ? "TRUE" : "FALSE";
          case "number":
            return (e | 0) === e ? e.toString(10) : zr(e);
          case "undefined":
            return "";
          case "object":
            if (e == null) return "";
            if (e instanceof Date) return He(14, Na(e, r && r.date1904), r);
        }
        throw new Error("unsupported value in General format: " + e);
      }
      function Qs(e, r) {
        r[0] -= 581;
        var t = e.getDay();
        return e < 60 && (t = (t + 6) % 7), t;
      }
      function ei(e, r, t, n) {
        var a = "", s = 0, i = 0, o = t.y, c, f = 0;
        switch (e) {
          case 98:
            o = t.y + 543;
          case 121:
            switch (r.length) {
              case 1:
              case 2:
                c = o % 100, f = 2;
                break;
              default:
                c = o % 1e4, f = 4;
                break;
            }
            break;
          case 109:
            switch (r.length) {
              case 1:
              case 2:
                c = t.m, f = r.length;
                break;
              case 3:
                return Lr[t.m - 1][1];
              case 5:
                return Lr[t.m - 1][0];
              default:
                return Lr[t.m - 1][2];
            }
            break;
          case 100:
            switch (r.length) {
              case 1:
              case 2:
                c = t.d, f = r.length;
                break;
              case 3:
                return Oa[t.q][0];
              default:
                return Oa[t.q][1];
            }
            break;
          case 104:
            switch (r.length) {
              case 1:
              case 2:
                c = 1 + (t.H + 11) % 12, f = r.length;
                break;
              default:
                throw "bad hour format: " + r;
            }
            break;
          case 72:
            switch (r.length) {
              case 1:
              case 2:
                c = t.H, f = r.length;
                break;
              default:
                throw "bad hour format: " + r;
            }
            break;
          case 77:
            switch (r.length) {
              case 1:
              case 2:
                c = t.M, f = r.length;
                break;
              default:
                throw "bad minute format: " + r;
            }
            break;
          case 115:
            if (r != "s" && r != "ss" && r != ".0" && r != ".00" && r != ".000") throw "bad second format: " + r;
            return t.u === 0 && (r == "s" || r == "ss") ? Be(t.S, r.length) : (n >= 2 ? i = n === 3 ? 1e3 : 100 : i = n === 1 ? 10 : 1, s = Math.round(i * (t.S + t.u)), s >= 60 * i && (s = 0), r === "s" ? s === 0 ? "0" : "" + s / i : (a = Be(s, 2 + n), r === "ss" ? a.substr(0, 2) : "." + a.substr(2, r.length - 1)));
          case 90:
            switch (r) {
              case "[h]":
              case "[hh]":
                c = t.D * 24 + t.H;
                break;
              case "[m]":
              case "[mm]":
                c = (t.D * 24 + t.H) * 60 + t.M;
                break;
              case "[s]":
              case "[ss]":
                c = ((t.D * 24 + t.H) * 60 + t.M) * 60 + Math.round(t.S + t.u);
                break;
              default:
                throw "bad abstime format: " + r;
            }
            f = r.length === 3 ? 1 : 2;
            break;
          case 101:
            c = o, f = 1;
            break;
        }
        var l = f > 0 ? Be(c, f) : "";
        return l;
      }
      function pt(e) {
        var r = 3;
        if (e.length <= r) return e;
        for (var t = e.length % r, n = e.substr(0, t); t != e.length; t += r) n += (n.length > 0 ? "," : "") + e.substr(t, r);
        return n;
      }
      var Pa = /%/g;
      function ti(e, r, t) {
        var n = r.replace(Pa, ""), a = r.length - n.length;
        return et(e, n, t * Math.pow(10, 2 * a)) + Re("%", a);
      }
      function ri(e, r, t) {
        for (var n = r.length - 1; r.charCodeAt(n - 1) === 44; ) --n;
        return et(e, r.substr(0, n), t / Math.pow(10, 3 * (r.length - n)));
      }
      function Wa(e, r) {
        var t, n = e.indexOf("E") - e.indexOf(".") - 1;
        if (e.match(/^#+0.0E\+0$/)) {
          if (r == 0) return "0.0E+0";
          if (r < 0) return "-" + Wa(e, -r);
          var a = e.indexOf(".");
          a === -1 && (a = e.indexOf("E"));
          var s = Math.floor(Math.log(r) * Math.LOG10E) % a;
          if (s < 0 && (s += a), t = (r / Math.pow(10, s)).toPrecision(n + 1 + (a + s) % a), t.indexOf("e") === -1) {
            var i = Math.floor(Math.log(r) * Math.LOG10E);
            for (t.indexOf(".") === -1 ? t = t.charAt(0) + "." + t.substr(1) + "E+" + (i - t.length + s) : t += "E+" + (i - s); t.substr(0, 2) === "0."; )
              t = t.charAt(0) + t.substr(2, a) + "." + t.substr(2 + a), t = t.replace(/^0+([1-9])/, "$1").replace(/^0+\./, "0.");
            t = t.replace(/\+-/, "-");
          }
          t = t.replace(/^([+-]?)(\d*)\.(\d*)[Ee]/, function(o, c, f, l) {
            return c + f + l.substr(0, (a + s) % a) + "." + l.substr(s) + "E";
          });
        } else t = r.toExponential(n);
        return e.match(/E\+00$/) && t.match(/e[+-]\d$/) && (t = t.substr(0, t.length - 1) + "0" + t.charAt(t.length - 1)), e.match(/E\-/) && t.match(/e\+/) && (t = t.replace(/e\+/, "e")), t.replace("e", "E");
      }
      var Ia = /# (\?+)( ?)\/( ?)(\d+)/;
      function ai(e, r, t) {
        var n = parseInt(e[4], 10), a = Math.round(r * n), s = Math.floor(a / n), i = a - s * n, o = n;
        return t + (s === 0 ? "" : "" + s) + " " + (i === 0 ? Re(" ", e[1].length + 1 + e[4].length) : Ir(i, e[1].length) + e[2] + "/" + e[3] + Be(o, e[4].length));
      }
      function ni(e, r, t) {
        return t + (r === 0 ? "" : "" + r) + Re(" ", e[1].length + 2 + e[4].length);
      }
      var La = /^#*0*\.([0#]+)/, $a = /\).*[0#]/, za = /\(###\) ###\\?-####/;
      function Ee(e) {
        for (var r = "", t, n = 0; n != e.length; ++n) switch (t = e.charCodeAt(n)) {
          case 35:
            break;
          case 63:
            r += " ";
            break;
          case 48:
            r += "0";
            break;
          default:
            r += String.fromCharCode(t);
        }
        return r;
      }
      function ja(e, r) {
        var t = Math.pow(10, r);
        return "" + Math.round(e * t) / t;
      }
      function Ua(e, r) {
        var t = e - Math.floor(e), n = Math.pow(10, r);
        return r < ("" + Math.round(t * n)).length ? 0 : Math.round(t * n);
      }
      function si(e, r) {
        return r < ("" + Math.round((e - Math.floor(e)) * Math.pow(10, r))).length ? 1 : 0;
      }
      function ii(e) {
        return e < 2147483647 && e > -2147483648 ? "" + (e >= 0 ? e | 0 : e - 1 | 0) : "" + Math.floor(e);
      }
      function We(e, r, t) {
        if (e.charCodeAt(0) === 40 && !r.match($a)) {
          var n = r.replace(/\( */, "").replace(/ \)/, "").replace(/\)/, "");
          return t >= 0 ? We("n", n, t) : "(" + We("n", n, -t) + ")";
        }
        if (r.charCodeAt(r.length - 1) === 44) return ri(e, r, t);
        if (r.indexOf("%") !== -1) return ti(e, r, t);
        if (r.indexOf("E") !== -1) return Wa(r, t);
        if (r.charCodeAt(0) === 36) return "$" + We(e, r.substr(r.charAt(1) == " " ? 2 : 1), t);
        var a, s, i, o, c = Math.abs(t), f = t < 0 ? "-" : "";
        if (r.match(/^00+$/)) return f + Ct(c, r.length);
        if (r.match(/^[#?]+$/))
          return a = Ct(t, 0), a === "0" && (a = ""), a.length > r.length ? a : Ee(r.substr(0, r.length - a.length)) + a;
        if (s = r.match(Ia)) return ai(s, c, f);
        if (r.match(/^#+0+$/)) return f + Ct(c, r.length - r.indexOf("0"));
        if (s = r.match(La))
          return a = ja(t, s[1].length).replace(/^([^\.]+)$/, "$1." + Ee(s[1])).replace(/\.$/, "." + Ee(s[1])).replace(/\.(\d*)$/, function(w, u) {
            return "." + u + Re("0", Ee(s[1]).length - u.length);
          }), r.indexOf("0.") !== -1 ? a : a.replace(/^0\./, ".");
        if (r = r.replace(/^#+([0.])/, "$1"), s = r.match(/^(0*)\.(#*)$/))
          return f + ja(c, s[2].length).replace(/\.(\d*[1-9])0*$/, ".$1").replace(/^(-?\d*)$/, "$1.").replace(/^0\./, s[1].length ? "0." : ".");
        if (s = r.match(/^#{1,3},##0(\.?)$/)) return f + pt(Ct(c, 0));
        if (s = r.match(/^#,##0\.([#0]*0)$/))
          return t < 0 ? "-" + We(e, r, -t) : pt("" + (Math.floor(t) + si(t, s[1].length))) + "." + Be(Ua(t, s[1].length), s[1].length);
        if (s = r.match(/^#,#*,#0/)) return We(e, r.replace(/^#,#*,/, ""), t);
        if (s = r.match(/^([0#]+)(\\?-([0#]+))+$/))
          return a = St(We(e, r.replace(/[\\-]/g, ""), t)), i = 0, St(St(r.replace(/\\/g, "")).replace(/[0#]/g, function(w) {
            return i < a.length ? a.charAt(i++) : w === "0" ? "0" : "";
          }));
        if (r.match(za))
          return a = We(e, "##########", t), "(" + a.substr(0, 3) + ") " + a.substr(3, 3) + "-" + a.substr(6);
        var l = "";
        if (s = r.match(/^([#0?]+)( ?)\/( ?)([#0?]+)/))
          return i = Math.min(s[4].length, 7), o = pr(c, Math.pow(10, i) - 1, !1), a = "" + f, l = et("n", s[1], o[1]), l.charAt(l.length - 1) == " " && (l = l.substr(0, l.length - 1) + "0"), a += l + s[2] + "/" + s[3], l = ur(o[2], i), l.length < s[4].length && (l = Ee(s[4].substr(s[4].length - l.length)) + l), a += l, a;
        if (s = r.match(/^# ([#0?]+)( ?)\/( ?)([#0?]+)/))
          return i = Math.min(Math.max(s[1].length, s[4].length), 7), o = pr(c, Math.pow(10, i) - 1, !0), f + (o[0] || (o[1] ? "" : "0")) + " " + (o[1] ? Ir(o[1], i) + s[2] + "/" + s[3] + ur(o[2], i) : Re(" ", 2 * i + 1 + s[2].length + s[3].length));
        if (s = r.match(/^[#0?]+$/))
          return a = Ct(t, 0), r.length <= a.length ? a : Ee(r.substr(0, r.length - a.length)) + a;
        if (s = r.match(/^([#0?]+)\.([#0]+)$/)) {
          a = "" + t.toFixed(Math.min(s[2].length, 10)).replace(/([^0])0+$/, "$1"), i = a.indexOf(".");
          var p = r.indexOf(".") - i, d = r.length - a.length - p;
          return Ee(r.substr(0, p) + a + r.substr(r.length - d));
        }
        if (s = r.match(/^00,000\.([#0]*0)$/))
          return i = Ua(t, s[1].length), t < 0 ? "-" + We(e, r, -t) : pt(ii(t)).replace(/^\d,\d{3}$/, "0$&").replace(/^\d*$/, function(w) {
            return "00," + (w.length < 3 ? Be(0, 3 - w.length) : "") + w;
          }) + "." + Be(i, s[1].length);
        switch (r) {
          case "###,##0.00":
            return We(e, "#,##0.00", t);
          case "###,###":
          case "##,###":
          case "#,###":
            var m = pt(Ct(c, 0));
            return m !== "0" ? f + m : "";
          case "###,###.00":
            return We(e, "###,##0.00", t).replace(/^0\./, ".");
          case "#,###.00":
            return We(e, "#,##0.00", t).replace(/^0\./, ".");
        }
        throw new Error("unsupported format |" + r + "|");
      }
      function oi(e, r, t) {
        for (var n = r.length - 1; r.charCodeAt(n - 1) === 44; ) --n;
        return et(e, r.substr(0, n), t / Math.pow(10, 3 * (r.length - n)));
      }
      function li(e, r, t) {
        var n = r.replace(Pa, ""), a = r.length - n.length;
        return et(e, n, t * Math.pow(10, 2 * a)) + Re("%", a);
      }
      function Ba(e, r) {
        var t, n = e.indexOf("E") - e.indexOf(".") - 1;
        if (e.match(/^#+0.0E\+0$/)) {
          if (r == 0) return "0.0E+0";
          if (r < 0) return "-" + Ba(e, -r);
          var a = e.indexOf(".");
          a === -1 && (a = e.indexOf("E"));
          var s = Math.floor(Math.log(r) * Math.LOG10E) % a;
          if (s < 0 && (s += a), t = (r / Math.pow(10, s)).toPrecision(n + 1 + (a + s) % a), !t.match(/[Ee]/)) {
            var i = Math.floor(Math.log(r) * Math.LOG10E);
            t.indexOf(".") === -1 ? t = t.charAt(0) + "." + t.substr(1) + "E+" + (i - t.length + s) : t += "E+" + (i - s), t = t.replace(/\+-/, "-");
          }
          t = t.replace(/^([+-]?)(\d*)\.(\d*)[Ee]/, function(o, c, f, l) {
            return c + f + l.substr(0, (a + s) % a) + "." + l.substr(s) + "E";
          });
        } else t = r.toExponential(n);
        return e.match(/E\+00$/) && t.match(/e[+-]\d$/) && (t = t.substr(0, t.length - 1) + "0" + t.charAt(t.length - 1)), e.match(/E\-/) && t.match(/e\+/) && (t = t.replace(/e\+/, "e")), t.replace("e", "E");
      }
      function Ge(e, r, t) {
        if (e.charCodeAt(0) === 40 && !r.match($a)) {
          var n = r.replace(/\( */, "").replace(/ \)/, "").replace(/\)/, "");
          return t >= 0 ? Ge("n", n, t) : "(" + Ge("n", n, -t) + ")";
        }
        if (r.charCodeAt(r.length - 1) === 44) return oi(e, r, t);
        if (r.indexOf("%") !== -1) return li(e, r, t);
        if (r.indexOf("E") !== -1) return Ba(r, t);
        if (r.charCodeAt(0) === 36) return "$" + Ge(e, r.substr(r.charAt(1) == " " ? 2 : 1), t);
        var a, s, i, o, c = Math.abs(t), f = t < 0 ? "-" : "";
        if (r.match(/^00+$/)) return f + Be(c, r.length);
        if (r.match(/^[#?]+$/))
          return a = "" + t, t === 0 && (a = ""), a.length > r.length ? a : Ee(r.substr(0, r.length - a.length)) + a;
        if (s = r.match(Ia)) return ni(s, c, f);
        if (r.match(/^#+0+$/)) return f + Be(c, r.length - r.indexOf("0"));
        if (s = r.match(La))
          return a = ("" + t).replace(/^([^\.]+)$/, "$1." + Ee(s[1])).replace(/\.$/, "." + Ee(s[1])), a = a.replace(/\.(\d*)$/, function(w, u) {
            return "." + u + Re("0", Ee(s[1]).length - u.length);
          }), r.indexOf("0.") !== -1 ? a : a.replace(/^0\./, ".");
        if (r = r.replace(/^#+([0.])/, "$1"), s = r.match(/^(0*)\.(#*)$/))
          return f + ("" + c).replace(/\.(\d*[1-9])0*$/, ".$1").replace(/^(-?\d*)$/, "$1.").replace(/^0\./, s[1].length ? "0." : ".");
        if (s = r.match(/^#{1,3},##0(\.?)$/)) return f + pt("" + c);
        if (s = r.match(/^#,##0\.([#0]*0)$/))
          return t < 0 ? "-" + Ge(e, r, -t) : pt("" + t) + "." + Re("0", s[1].length);
        if (s = r.match(/^#,#*,#0/)) return Ge(e, r.replace(/^#,#*,/, ""), t);
        if (s = r.match(/^([0#]+)(\\?-([0#]+))+$/))
          return a = St(Ge(e, r.replace(/[\\-]/g, ""), t)), i = 0, St(St(r.replace(/\\/g, "")).replace(/[0#]/g, function(w) {
            return i < a.length ? a.charAt(i++) : w === "0" ? "0" : "";
          }));
        if (r.match(za))
          return a = Ge(e, "##########", t), "(" + a.substr(0, 3) + ") " + a.substr(3, 3) + "-" + a.substr(6);
        var l = "";
        if (s = r.match(/^([#0?]+)( ?)\/( ?)([#0?]+)/))
          return i = Math.min(s[4].length, 7), o = pr(c, Math.pow(10, i) - 1, !1), a = "" + f, l = et("n", s[1], o[1]), l.charAt(l.length - 1) == " " && (l = l.substr(0, l.length - 1) + "0"), a += l + s[2] + "/" + s[3], l = ur(o[2], i), l.length < s[4].length && (l = Ee(s[4].substr(s[4].length - l.length)) + l), a += l, a;
        if (s = r.match(/^# ([#0?]+)( ?)\/( ?)([#0?]+)/))
          return i = Math.min(Math.max(s[1].length, s[4].length), 7), o = pr(c, Math.pow(10, i) - 1, !0), f + (o[0] || (o[1] ? "" : "0")) + " " + (o[1] ? Ir(o[1], i) + s[2] + "/" + s[3] + ur(o[2], i) : Re(" ", 2 * i + 1 + s[2].length + s[3].length));
        if (s = r.match(/^[#0?]+$/))
          return a = "" + t, r.length <= a.length ? a : Ee(r.substr(0, r.length - a.length)) + a;
        if (s = r.match(/^([#0]+)\.([#0]+)$/)) {
          a = "" + t.toFixed(Math.min(s[2].length, 10)).replace(/([^0])0+$/, "$1"), i = a.indexOf(".");
          var p = r.indexOf(".") - i, d = r.length - a.length - p;
          return Ee(r.substr(0, p) + a + r.substr(r.length - d));
        }
        if (s = r.match(/^00,000\.([#0]*0)$/))
          return t < 0 ? "-" + Ge(e, r, -t) : pt("" + t).replace(/^\d,\d{3}$/, "0$&").replace(/^\d*$/, function(w) {
            return "00," + (w.length < 3 ? Be(0, 3 - w.length) : "") + w;
          }) + "." + Be(0, s[1].length);
        switch (r) {
          case "###,###":
          case "##,###":
          case "#,###":
            var m = pt("" + c);
            return m !== "0" ? f + m : "";
          default:
            if (r.match(/\.[0#?]*$/)) return Ge(e, r.slice(0, r.lastIndexOf(".")), t) + Ee(r.slice(r.lastIndexOf(".")));
        }
        throw new Error("unsupported format |" + r + "|");
      }
      function et(e, r, t) {
        return (t | 0) === t ? Ge(e, r, t) : We(e, r, t);
      }
      function ci(e) {
        for (var r = [], t = !1, n = 0, a = 0; n < e.length; ++n) switch (e.charCodeAt(n)) {
          case 34:
            t = !t;
            break;
          case 95:
          case 42:
          case 92:
            ++n;
            break;
          case 59:
            r[r.length] = e.substr(a, n - a), a = n + 1;
        }
        if (r[r.length] = e.substr(a), t === !0) throw new Error("Format |" + e + "| unterminated string ");
        return r;
      }
      var Ha = /\[[HhMmSs\u0E0A\u0E19\u0E17]*\]/;
      function dr(e) {
        for (var r = 0, t = "", n = ""; r < e.length; )
          switch (t = e.charAt(r)) {
            case "G":
              hr(e, r) && (r += 6), r++;
              break;
            case '"':
              for (; e.charCodeAt(++r) !== 34 && r < e.length; )
                ;
              ++r;
              break;
            case "\\":
              r += 2;
              break;
            case "_":
              r += 2;
              break;
            case "@":
              ++r;
              break;
            case "B":
            case "b":
              if (e.charAt(r + 1) === "1" || e.charAt(r + 1) === "2") return !0;
            case "M":
            case "D":
            case "Y":
            case "H":
            case "S":
            case "E":
            case "m":
            case "d":
            case "y":
            case "h":
            case "s":
            case "e":
            case "g":
              return !0;
            case "A":
            case "a":
            case "上":
              if (e.substr(r, 3).toUpperCase() === "A/P" || e.substr(r, 5).toUpperCase() === "AM/PM" || e.substr(r, 5).toUpperCase() === "上午/下午") return !0;
              ++r;
              break;
            case "[":
              for (n = t; e.charAt(r++) !== "]" && r < e.length; ) n += e.charAt(r);
              if (n.match(Ha)) return !0;
              break;
            case ".":
            case "0":
            case "#":
              for (; r < e.length && ("0#?.,E+-%".indexOf(t = e.charAt(++r)) > -1 || t == "\\" && e.charAt(r + 1) == "-" && "0#".indexOf(e.charAt(r + 2)) > -1); )
                ;
              break;
            case "?":
              for (; e.charAt(++r) === t; )
                ;
              break;
            case "*":
              ++r, (e.charAt(r) == " " || e.charAt(r) == "*") && ++r;
              break;
            case "(":
            case ")":
              ++r;
              break;
            case "1":
            case "2":
            case "3":
            case "4":
            case "5":
            case "6":
            case "7":
            case "8":
            case "9":
              for (; r < e.length && "0123456789".indexOf(e.charAt(++r)) > -1; )
                ;
              break;
            case " ":
              ++r;
              break;
            default:
              ++r;
              break;
          }
        return !1;
      }
      function fi(e, r, t, n) {
        for (var a = [], s = "", i = 0, o = "", c = "t", f, l, p, d = "H"; i < e.length; )
          switch (o = e.charAt(i)) {
            case "G":
              if (!hr(e, i)) throw new Error("unrecognized character " + o + " in " + e);
              a[a.length] = { t: "G", v: "General" }, i += 7;
              break;
            case '"':
              for (s = ""; (p = e.charCodeAt(++i)) !== 34 && i < e.length; ) s += String.fromCharCode(p);
              a[a.length] = { t: "t", v: s }, ++i;
              break;
            case "\\":
              var m = e.charAt(++i), w = m === "(" || m === ")" ? m : "t";
              a[a.length] = { t: w, v: m }, ++i;
              break;
            case "_":
              a[a.length] = { t: "t", v: " " }, i += 2;
              break;
            case "@":
              a[a.length] = { t: "T", v: r }, ++i;
              break;
            case "B":
            case "b":
              if (e.charAt(i + 1) === "1" || e.charAt(i + 1) === "2") {
                if (f == null && (f = Bt(r, t, e.charAt(i + 1) === "2"), f == null))
                  return "";
                a[a.length] = { t: "X", v: e.substr(i, 2) }, c = o, i += 2;
                break;
              }
            case "M":
            case "D":
            case "Y":
            case "H":
            case "S":
            case "E":
              o = o.toLowerCase();
            case "m":
            case "d":
            case "y":
            case "h":
            case "s":
            case "e":
            case "g":
              if (r < 0 || f == null && (f = Bt(r, t), f == null))
                return "";
              for (s = o; ++i < e.length && e.charAt(i).toLowerCase() === o; ) s += o;
              o === "m" && c.toLowerCase() === "h" && (o = "M"), o === "h" && (o = d), a[a.length] = { t: o, v: s }, c = o;
              break;
            case "A":
            case "a":
            case "上":
              var u = { t: o, v: o };
              if (f == null && (f = Bt(r, t)), e.substr(i, 3).toUpperCase() === "A/P" ? (f != null && (u.v = f.H >= 12 ? "P" : "A"), u.t = "T", d = "h", i += 3) : e.substr(i, 5).toUpperCase() === "AM/PM" ? (f != null && (u.v = f.H >= 12 ? "PM" : "AM"), u.t = "T", i += 5, d = "h") : e.substr(i, 5).toUpperCase() === "上午/下午" ? (f != null && (u.v = f.H >= 12 ? "下午" : "上午"), u.t = "T", i += 5, d = "h") : (u.t = "t", ++i), f == null && u.t === "T") return "";
              a[a.length] = u, c = o;
              break;
            case "[":
              for (s = o; e.charAt(i++) !== "]" && i < e.length; ) s += e.charAt(i);
              if (s.slice(-1) !== "]") throw 'unterminated "[" block: |' + s + "|";
              if (s.match(Ha)) {
                if (f == null && (f = Bt(r, t), f == null))
                  return "";
                a[a.length] = { t: "Z", v: s.toLowerCase() }, c = s.charAt(1);
              } else s.indexOf("$") > -1 && (s = (s.match(/\$([^-\[\]]*)/) || [])[1] || "$", dr(e) || (a[a.length] = { t: "t", v: s }));
              break;
            case ".":
              if (f != null) {
                for (s = o; ++i < e.length && (o = e.charAt(i)) === "0"; ) s += o;
                a[a.length] = { t: "s", v: s };
                break;
              }
            case "0":
            case "#":
              for (s = o; ++i < e.length && "0#?.,E+-%".indexOf(o = e.charAt(i)) > -1; ) s += o;
              a[a.length] = { t: "n", v: s };
              break;
            case "?":
              for (s = o; e.charAt(++i) === o; ) s += o;
              a[a.length] = { t: o, v: s }, c = o;
              break;
            case "*":
              ++i, (e.charAt(i) == " " || e.charAt(i) == "*") && ++i;
              break;
            case "(":
            case ")":
              a[a.length] = { t: n === 1 ? "t" : o, v: o }, ++i;
              break;
            case "1":
            case "2":
            case "3":
            case "4":
            case "5":
            case "6":
            case "7":
            case "8":
            case "9":
              for (s = o; i < e.length && "0123456789".indexOf(e.charAt(++i)) > -1; ) s += e.charAt(i);
              a[a.length] = { t: "D", v: s };
              break;
            case " ":
              a[a.length] = { t: o, v: o }, ++i;
              break;
            case "$":
              a[a.length] = { t: "t", v: "$" }, ++i;
              break;
            default:
              if (",$-+/():!^&'~{}<>=€acfijklopqrtuvwxzP".indexOf(o) === -1) throw new Error("unrecognized character " + o + " in " + e);
              a[a.length] = { t: "t", v: o }, ++i;
              break;
          }
        var g = 0, C = 0, _;
        for (i = a.length - 1, c = "t"; i >= 0; --i)
          switch (a[i].t) {
            case "h":
            case "H":
              a[i].t = d, c = "h", g < 1 && (g = 1);
              break;
            case "s":
              (_ = a[i].v.match(/\.0+$/)) && (C = Math.max(C, _[0].length - 1)), g < 3 && (g = 3);
            case "d":
            case "y":
            case "M":
            case "e":
              c = a[i].t;
              break;
            case "m":
              c === "s" && (a[i].t = "M", g < 2 && (g = 2));
              break;
            case "X":
              break;
            case "Z":
              g < 1 && a[i].v.match(/[Hh]/) && (g = 1), g < 2 && a[i].v.match(/[Mm]/) && (g = 2), g < 3 && a[i].v.match(/[Ss]/) && (g = 3);
          }
        switch (g) {
          case 0:
            break;
          case 1:
            f.u >= 0.5 && (f.u = 0, ++f.S), f.S >= 60 && (f.S = 0, ++f.M), f.M >= 60 && (f.M = 0, ++f.H);
            break;
          case 2:
            f.u >= 0.5 && (f.u = 0, ++f.S), f.S >= 60 && (f.S = 0, ++f.M);
            break;
        }
        var E = "", R;
        for (i = 0; i < a.length; ++i)
          switch (a[i].t) {
            case "t":
            case "T":
            case " ":
            case "D":
              break;
            case "X":
              a[i].v = "", a[i].t = ";";
              break;
            case "d":
            case "m":
            case "y":
            case "h":
            case "H":
            case "M":
            case "s":
            case "e":
            case "b":
            case "Z":
              a[i].v = ei(a[i].t.charCodeAt(0), a[i].v, f, C), a[i].t = "t";
              break;
            case "n":
            case "?":
              for (R = i + 1; a[R] != null && ((o = a[R].t) === "?" || o === "D" || (o === " " || o === "t") && a[R + 1] != null && (a[R + 1].t === "?" || a[R + 1].t === "t" && a[R + 1].v === "/") || a[i].t === "(" && (o === " " || o === "n" || o === ")") || o === "t" && (a[R].v === "/" || a[R].v === " " && a[R + 1] != null && a[R + 1].t == "?")); )
                a[i].v += a[R].v, a[R] = { v: "", t: ";" }, ++R;
              E += a[i].v, i = R - 1;
              break;
            case "G":
              a[i].t = "t", a[i].v = mr(r, t);
              break;
          }
        var N = "", F, S;
        if (E.length > 0) {
          E.charCodeAt(0) == 40 ? (F = r < 0 && E.charCodeAt(0) === 45 ? -r : r, S = et("n", E, F)) : (F = r < 0 && n > 1 ? -r : r, S = et("n", E, F), F < 0 && a[0] && a[0].t == "t" && (S = S.substr(1), a[0].v = "-" + a[0].v)), R = S.length - 1;
          var W = a.length;
          for (i = 0; i < a.length; ++i) if (a[i] != null && a[i].t != "t" && a[i].v.indexOf(".") > -1) {
            W = i;
            break;
          }
          var M = a.length;
          if (W === a.length && S.indexOf("E") === -1) {
            for (i = a.length - 1; i >= 0; --i)
              a[i] == null || "n?".indexOf(a[i].t) === -1 || (R >= a[i].v.length - 1 ? (R -= a[i].v.length, a[i].v = S.substr(R + 1, a[i].v.length)) : R < 0 ? a[i].v = "" : (a[i].v = S.substr(0, R + 1), R = -1), a[i].t = "t", M = i);
            R >= 0 && M < a.length && (a[M].v = S.substr(0, R + 1) + a[M].v);
          } else if (W !== a.length && S.indexOf("E") === -1) {
            for (R = S.indexOf(".") - 1, i = W; i >= 0; --i)
              if (!(a[i] == null || "n?".indexOf(a[i].t) === -1)) {
                for (l = a[i].v.indexOf(".") > -1 && i === W ? a[i].v.indexOf(".") - 1 : a[i].v.length - 1, N = a[i].v.substr(l + 1); l >= 0; --l)
                  R >= 0 && (a[i].v.charAt(l) === "0" || a[i].v.charAt(l) === "#") && (N = S.charAt(R--) + N);
                a[i].v = N, a[i].t = "t", M = i;
              }
            for (R >= 0 && M < a.length && (a[M].v = S.substr(0, R + 1) + a[M].v), R = S.indexOf(".") + 1, i = W; i < a.length; ++i)
              if (!(a[i] == null || "n?(".indexOf(a[i].t) === -1 && i !== W)) {
                for (l = a[i].v.indexOf(".") > -1 && i === W ? a[i].v.indexOf(".") + 1 : 0, N = a[i].v.substr(0, l); l < a[i].v.length; ++l)
                  R < S.length && (N += S.charAt(R++));
                a[i].v = N, a[i].t = "t", M = i;
              }
          }
        }
        for (i = 0; i < a.length; ++i) a[i] != null && "n?".indexOf(a[i].t) > -1 && (F = n > 1 && r < 0 && i > 0 && a[i - 1].v === "-" ? -r : r, a[i].v = et(a[i].t, a[i].v, F), a[i].t = "t");
        var L = "";
        for (i = 0; i !== a.length; ++i) a[i] != null && (L += a[i].v);
        return L;
      }
      var Va = /\[(=|>[=]?|<[>=]?)(-?\d+(?:\.\d*)?)\]/;
      function Xa(e, r) {
        if (r == null) return !1;
        var t = parseFloat(r[2]);
        switch (r[1]) {
          case "=":
            if (e == t) return !0;
            break;
          case ">":
            if (e > t) return !0;
            break;
          case "<":
            if (e < t) return !0;
            break;
          case "<>":
            if (e != t) return !0;
            break;
          case ">=":
            if (e >= t) return !0;
            break;
          case "<=":
            if (e <= t) return !0;
            break;
        }
        return !1;
      }
      function ui(e, r) {
        var t = ci(e), n = t.length, a = t[n - 1].indexOf("@");
        if (n < 4 && a > -1 && --n, t.length > 4) throw new Error("cannot find right format for |" + t.join("|") + "|");
        if (typeof r != "number") return [4, t.length === 4 || a > -1 ? t[t.length - 1] : "@"];
        switch (t.length) {
          case 1:
            t = a > -1 ? ["General", "General", "General", t[0]] : [t[0], t[0], t[0], "@"];
            break;
          case 2:
            t = a > -1 ? [t[0], t[0], t[0], t[1]] : [t[0], t[1], t[0], "@"];
            break;
          case 3:
            t = a > -1 ? [t[0], t[1], t[0], t[2]] : [t[0], t[1], t[2], "@"];
            break;
        }
        var s = r > 0 ? t[0] : r < 0 ? t[1] : t[2];
        if (t[0].indexOf("[") === -1 && t[1].indexOf("[") === -1) return [n, s];
        if (t[0].match(/\[[=<>]/) != null || t[1].match(/\[[=<>]/) != null) {
          var i = t[0].match(Va), o = t[1].match(Va);
          return Xa(r, i) ? [n, t[0]] : Xa(r, o) ? [n, t[1]] : [n, t[i != null && o != null ? 2 : 1]];
        }
        return [n, s];
      }
      function He(e, r, t) {
        t == null && (t = {});
        var n = "";
        switch (typeof e) {
          case "string":
            e == "m/d/yy" && t.dateNF ? n = t.dateNF : n = e;
            break;
          case "number":
            e == 14 && t.dateNF ? n = t.dateNF : n = (t.table != null ? t.table : ce)[e], n == null && (n = t.table && t.table[Ma[e]] || ce[Ma[e]]), n == null && (n = Gs[e] || "General");
            break;
        }
        if (hr(n, 0)) return mr(r, t);
        r instanceof Date && (r = Na(r, t.date1904));
        var a = ui(n, r);
        if (hr(a[1])) return mr(r, t);
        if (r === !0) r = "TRUE";
        else if (r === !1) r = "FALSE";
        else if (r === "" || r == null) return "";
        return fi(a[1], r, t, a[0]);
      }
      function Ht(e, r) {
        if (typeof r != "number") {
          r = +r || -1;
          for (var t = 0; t < 392; ++t) {
            if (ce[t] == null) {
              r < 0 && (r = t);
              continue;
            }
            if (ce[t] == e) {
              r = t;
              break;
            }
          }
          r < 0 && (r = 391);
        }
        return ce[r] = e, r;
      }
      function jr(e) {
        for (var r = 0; r != 392; ++r) e[r] !== void 0 && Ht(e[r], r);
      }
      function Ur() {
        ce = Xs();
      }
      var Ga = { format: He, load: Ht, _table: ce, load_table: jr, parse_date_code: Bt, is_date: dr, get_table: function() {
        return Ga._table = ce;
      } }, hi = { 5: '"$"#,##0_);\\("$"#,##0\\)', 6: '"$"#,##0_);[Red]\\("$"#,##0\\)', 7: '"$"#,##0.00_);\\("$"#,##0.00\\)', 8: '"$"#,##0.00_);[Red]\\("$"#,##0.00\\)', 23: "General", 24: "General", 25: "General", 26: "General", 27: "m/d/yy", 28: "m/d/yy", 29: "m/d/yy", 30: "m/d/yy", 31: "m/d/yy", 32: "h:mm:ss", 33: "h:mm:ss", 34: "h:mm:ss", 35: "h:mm:ss", 36: "m/d/yy", 41: '_(* #,##0_);_(* (#,##0);_(* "-"_);_(@_)', 42: '_("$"* #,##0_);_("$"* (#,##0);_("$"* "-"_);_(@_)', 43: '_(* #,##0.00_);_(* (#,##0.00);_(* "-"??_);_(@_)', 44: '_("$"* #,##0.00_);_("$"* (#,##0.00);_("$"* "-"??_);_(@_)', 50: "m/d/yy", 51: "m/d/yy", 52: "m/d/yy", 53: "m/d/yy", 54: "m/d/yy", 55: "m/d/yy", 56: "m/d/yy", 57: "m/d/yy", 58: "m/d/yy", 59: "0", 60: "0.00", 61: "#,##0", 62: "#,##0.00", 63: '"$"#,##0_);\\("$"#,##0\\)', 64: '"$"#,##0_);[Red]\\("$"#,##0\\)', 65: '"$"#,##0.00_);\\("$"#,##0.00\\)', 66: '"$"#,##0.00_);[Red]\\("$"#,##0.00\\)', 67: "0%", 68: "0.00%", 69: "# ?/?", 70: "# ??/??", 71: "m/d/yy", 72: "m/d/yy", 73: "d-mmm-yy", 74: "d-mmm", 75: "mmm-yy", 76: "h:mm", 77: "h:mm:ss", 78: "m/d/yy h:mm", 79: "mm:ss", 80: "[h]:mm:ss", 81: "mmss.0" }, Ya = /[dD]+|[mM]+|[yYeE]+|[Hh]+|[Ss]+/g;
      function pi(e) {
        var r = typeof e == "number" ? ce[e] : e;
        return r = r.replace(Ya, "(\\d+)"), new RegExp("^" + r + "$");
      }
      function mi(e, r, t) {
        var n = -1, a = -1, s = -1, i = -1, o = -1, c = -1;
        (r.match(Ya) || []).forEach(function(p, d) {
          var m = parseInt(t[d + 1], 10);
          switch (p.toLowerCase().charAt(0)) {
            case "y":
              n = m;
              break;
            case "d":
              s = m;
              break;
            case "h":
              i = m;
              break;
            case "s":
              c = m;
              break;
            case "m":
              i >= 0 ? o = m : a = m;
              break;
          }
        }), c >= 0 && o == -1 && a >= 0 && (o = a, a = -1);
        var f = ("" + (n >= 0 ? n : (/* @__PURE__ */ new Date()).getFullYear())).slice(-4) + "-" + ("00" + (a >= 1 ? a : 1)).slice(-2) + "-" + ("00" + (s >= 1 ? s : 1)).slice(-2);
        f.length == 7 && (f = "0" + f), f.length == 8 && (f = "20" + f);
        var l = ("00" + (i >= 0 ? i : 0)).slice(-2) + ":" + ("00" + (o >= 0 ? o : 0)).slice(-2) + ":" + ("00" + (c >= 0 ? c : 0)).slice(-2);
        return i == -1 && o == -1 && c == -1 ? f : n == -1 && a == -1 && s == -1 ? l : f + "T" + l;
      }
      var di = (function() {
        var e = {};
        e.version = "1.2.0";
        function r() {
          for (var S = 0, W = new Array(256), M = 0; M != 256; ++M)
            S = M, S = S & 1 ? -306674912 ^ S >>> 1 : S >>> 1, S = S & 1 ? -306674912 ^ S >>> 1 : S >>> 1, S = S & 1 ? -306674912 ^ S >>> 1 : S >>> 1, S = S & 1 ? -306674912 ^ S >>> 1 : S >>> 1, S = S & 1 ? -306674912 ^ S >>> 1 : S >>> 1, S = S & 1 ? -306674912 ^ S >>> 1 : S >>> 1, S = S & 1 ? -306674912 ^ S >>> 1 : S >>> 1, S = S & 1 ? -306674912 ^ S >>> 1 : S >>> 1, W[M] = S;
          return typeof Int32Array < "u" ? new Int32Array(W) : W;
        }
        var t = r();
        function n(S) {
          var W = 0, M = 0, L = 0, I = typeof Int32Array < "u" ? new Int32Array(4096) : new Array(4096);
          for (L = 0; L != 256; ++L) I[L] = S[L];
          for (L = 0; L != 256; ++L)
            for (M = S[L], W = 256 + L; W < 4096; W += 256) M = I[W] = M >>> 8 ^ S[M & 255];
          var z = [];
          for (L = 1; L != 16; ++L) z[L - 1] = typeof Int32Array < "u" ? I.subarray(L * 256, L * 256 + 256) : I.slice(L * 256, L * 256 + 256);
          return z;
        }
        var a = n(t), s = a[0], i = a[1], o = a[2], c = a[3], f = a[4], l = a[5], p = a[6], d = a[7], m = a[8], w = a[9], u = a[10], g = a[11], C = a[12], _ = a[13], E = a[14];
        function R(S, W) {
          for (var M = W ^ -1, L = 0, I = S.length; L < I; ) M = M >>> 8 ^ t[(M ^ S.charCodeAt(L++)) & 255];
          return ~M;
        }
        function N(S, W) {
          for (var M = W ^ -1, L = S.length - 15, I = 0; I < L; ) M = E[S[I++] ^ M & 255] ^ _[S[I++] ^ M >> 8 & 255] ^ C[S[I++] ^ M >> 16 & 255] ^ g[S[I++] ^ M >>> 24] ^ u[S[I++]] ^ w[S[I++]] ^ m[S[I++]] ^ d[S[I++]] ^ p[S[I++]] ^ l[S[I++]] ^ f[S[I++]] ^ c[S[I++]] ^ o[S[I++]] ^ i[S[I++]] ^ s[S[I++]] ^ t[S[I++]];
          for (L += 15; I < L; ) M = M >>> 8 ^ t[(M ^ S[I++]) & 255];
          return ~M;
        }
        function F(S, W) {
          for (var M = W ^ -1, L = 0, I = S.length, z = 0, G = 0; L < I; )
            z = S.charCodeAt(L++), z < 128 ? M = M >>> 8 ^ t[(M ^ z) & 255] : z < 2048 ? (M = M >>> 8 ^ t[(M ^ (192 | z >> 6 & 31)) & 255], M = M >>> 8 ^ t[(M ^ (128 | z & 63)) & 255]) : z >= 55296 && z < 57344 ? (z = (z & 1023) + 64, G = S.charCodeAt(L++) & 1023, M = M >>> 8 ^ t[(M ^ (240 | z >> 8 & 7)) & 255], M = M >>> 8 ^ t[(M ^ (128 | z >> 2 & 63)) & 255], M = M >>> 8 ^ t[(M ^ (128 | G >> 6 & 15 | (z & 3) << 4)) & 255], M = M >>> 8 ^ t[(M ^ (128 | G & 63)) & 255]) : (M = M >>> 8 ^ t[(M ^ (224 | z >> 12 & 15)) & 255], M = M >>> 8 ^ t[(M ^ (128 | z >> 6 & 63)) & 255], M = M >>> 8 ^ t[(M ^ (128 | z & 63)) & 255]);
          return ~M;
        }
        return e.table = t, e.bstr = R, e.buf = N, e.str = F, e;
      })(), se = (function() {
        var r = {};
        r.version = "1.2.1";
        function t(h, y) {
          for (var v = h.split("/"), b = y.split("/"), x = 0, k = 0, D = Math.min(v.length, b.length); x < D; ++x) {
            if (k = v[x].length - b[x].length) return k;
            if (v[x] != b[x]) return v[x] < b[x] ? -1 : 1;
          }
          return v.length - b.length;
        }
        function n(h) {
          if (h.charAt(h.length - 1) == "/") return h.slice(0, -1).indexOf("/") === -1 ? h : n(h.slice(0, -1));
          var y = h.lastIndexOf("/");
          return y === -1 ? h : h.slice(0, y + 1);
        }
        function a(h) {
          if (h.charAt(h.length - 1) == "/") return a(h.slice(0, -1));
          var y = h.lastIndexOf("/");
          return y === -1 ? h : h.slice(y + 1);
        }
        function s(h, y) {
          typeof y == "string" && (y = new Date(y));
          var v = y.getHours();
          v = v << 6 | y.getMinutes(), v = v << 5 | y.getSeconds() >>> 1, h._W(2, v);
          var b = y.getFullYear() - 1980;
          b = b << 4 | y.getMonth() + 1, b = b << 5 | y.getDate(), h._W(2, b);
        }
        function i(h) {
          var y = h._R(2) & 65535, v = h._R(2) & 65535, b = /* @__PURE__ */ new Date(), x = v & 31;
          v >>>= 5;
          var k = v & 15;
          v >>>= 4, b.setMilliseconds(0), b.setFullYear(v + 1980), b.setMonth(k - 1), b.setDate(x);
          var D = y & 31;
          y >>>= 5;
          var P = y & 63;
          return y >>>= 6, b.setHours(y), b.setMinutes(P), b.setSeconds(D << 1), b;
        }
        function o(h) {
          $e(h, 0);
          for (var y = {}, v = 0; h.l <= h.length - 4; ) {
            var b = h._R(2), x = h._R(2), k = h.l + x, D = {};
            switch (b) {
              case 21589:
                v = h._R(1), v & 1 && (D.mtime = h._R(4)), x > 5 && (v & 2 && (D.atime = h._R(4)), v & 4 && (D.ctime = h._R(4))), D.mtime && (D.mt = new Date(D.mtime * 1e3));
                break;
            }
            h.l = k, y[b] = D;
          }
          return y;
        }
        var c;
        function f() {
          return c || (c = void 0);
        }
        function l(h, y) {
          if (h[0] == 80 && h[1] == 75) return Rs(h, y);
          if ((h[0] | 32) == 109 && (h[1] | 32) == 105) return If(h, y);
          if (h.length < 512) throw new Error("CFB file size " + h.length + " < 512");
          var v = 3, b = 512, x = 0, k = 0, D = 0, P = 0, A = 0, T = [], O = h.slice(0, 512);
          $e(O, 0);
          var j = p(O);
          switch (v = j[0], v) {
            case 3:
              b = 512;
              break;
            case 4:
              b = 4096;
              break;
            case 0:
              if (j[1] == 0) return Rs(h, y);
            default:
              throw new Error("Major Version: Expected 3 or 4 saw " + v);
          }
          b !== 512 && (O = h.slice(0, b), $e(O, 28));
          var B = h.slice(0, b);
          d(O, v);
          var V = O._R(4, "i");
          if (v === 3 && V !== 0) throw new Error("# Directory Sectors: Expected 0 saw " + V);
          O.l += 4, D = O._R(4, "i"), O.l += 4, O.chk("00100000", "Mini Stream Cutoff Size: "), P = O._R(4, "i"), x = O._R(4, "i"), A = O._R(4, "i"), k = O._R(4, "i");
          for (var U = -1, H = 0; H < 109 && (U = O._R(4, "i"), !(U < 0)); ++H)
            T[H] = U;
          var q = m(h, b);
          g(A, k, q, b, T);
          var pe = _(q, D, T, b);
          pe[D].name = "!Directory", x > 0 && P !== G && (pe[P].name = "!MiniFAT"), pe[T[0]].name = "!FAT", pe.fat_addrs = T, pe.ssz = b;
          var me = {}, Te = [], nr = [], sr = [];
          E(D, pe, q, Te, x, me, nr, P), w(nr, sr, Te), Te.shift();
          var ir = { FileIndex: nr, FullPaths: sr };
          return y && y.raw && (ir.raw = { header: B, sectors: q }), ir;
        }
        function p(h) {
          if (h[h.l] == 80 && h[h.l + 1] == 75) return [0, 0];
          h.chk(Q, "Header Signature: "), h.l += 16;
          var y = h._R(2, "u");
          return [h._R(2, "u"), y];
        }
        function d(h, y) {
          var v = 9;
          switch (h.l += 2, v = h._R(2)) {
            case 9:
              if (y != 3) throw new Error("Sector Shift: Expected 9 saw " + v);
              break;
            case 12:
              if (y != 4) throw new Error("Sector Shift: Expected 12 saw " + v);
              break;
            default:
              throw new Error("Sector Shift: Expected 9 or 12 saw " + v);
          }
          h.chk("0600", "Mini Sector Shift: "), h.chk("000000000000", "Reserved: ");
        }
        function m(h, y) {
          for (var v = Math.ceil(h.length / y) - 1, b = [], x = 1; x < v; ++x) b[x - 1] = h.slice(x * y, (x + 1) * y);
          return b[v - 1] = h.slice(v * y), b;
        }
        function w(h, y, v) {
          for (var b = 0, x = 0, k = 0, D = 0, P = 0, A = v.length, T = [], O = []; b < A; ++b)
            T[b] = O[b] = b, y[b] = v[b];
          for (; P < O.length; ++P)
            b = O[P], x = h[b].L, k = h[b].R, D = h[b].C, T[b] === b && (x !== -1 && T[x] !== x && (T[b] = T[x]), k !== -1 && T[k] !== k && (T[b] = T[k])), D !== -1 && (T[D] = b), x !== -1 && b != T[b] && (T[x] = T[b], O.lastIndexOf(x) < P && O.push(x)), k !== -1 && b != T[b] && (T[k] = T[b], O.lastIndexOf(k) < P && O.push(k));
          for (b = 1; b < A; ++b) T[b] === b && (k !== -1 && T[k] !== k ? T[b] = T[k] : x !== -1 && T[x] !== x && (T[b] = T[x]));
          for (b = 1; b < A; ++b)
            if (h[b].type !== 0) {
              if (P = b, P != T[P]) do
                P = T[P], y[b] = y[P] + "/" + y[b];
              while (P !== 0 && T[P] !== -1 && P != T[P]);
              T[b] = -1;
            }
          for (y[0] += "/", b = 1; b < A; ++b)
            h[b].type !== 2 && (y[b] += "/");
        }
        function u(h, y, v) {
          for (var b = h.start, x = h.size, k = [], D = b; v && x > 0 && D >= 0; )
            k.push(y.slice(D * z, D * z + z)), x -= z, D = Tt(v, D * 4);
          return k.length === 0 ? Ye(0) : vt(k).slice(0, h.size);
        }
        function g(h, y, v, b, x) {
          var k = G;
          if (h === G) {
            if (y !== 0) throw new Error("DIFAT chain shorter than expected");
          } else if (h !== -1) {
            var D = v[h], P = (b >>> 2) - 1;
            if (!D) return;
            for (var A = 0; A < P && (k = Tt(D, A * 4)) !== G; ++A)
              x.push(k);
            g(Tt(D, b - 4), y - 1, v, b, x);
          }
        }
        function C(h, y, v, b, x) {
          var k = [], D = [];
          x || (x = []);
          var P = b - 1, A = 0, T = 0;
          for (A = y; A >= 0; ) {
            x[A] = !0, k[k.length] = A, D.push(h[A]);
            var O = v[Math.floor(A * 4 / b)];
            if (T = A * 4 & P, b < 4 + T) throw new Error("FAT boundary crossed: " + A + " 4 " + b);
            if (!h[O]) break;
            A = Tt(h[O], T);
          }
          return { nodes: k, data: hn([D]) };
        }
        function _(h, y, v, b) {
          var x = h.length, k = [], D = [], P = [], A = [], T = b - 1, O = 0, j = 0, B = 0, V = 0;
          for (O = 0; O < x; ++O)
            if (P = [], B = O + y, B >= x && (B -= x), !D[B]) {
              A = [];
              var U = [];
              for (j = B; j >= 0; ) {
                U[j] = !0, D[j] = !0, P[P.length] = j, A.push(h[j]);
                var H = v[Math.floor(j * 4 / b)];
                if (V = j * 4 & T, b < 4 + V) throw new Error("FAT boundary crossed: " + j + " 4 " + b);
                if (!h[H] || (j = Tt(h[H], V), U[j])) break;
              }
              k[B] = { nodes: P, data: hn([A]) };
            }
          return k;
        }
        function E(h, y, v, b, x, k, D, P) {
          for (var A = 0, T = b.length ? 2 : 0, O = y[h].data, j = 0, B = 0, V; j < O.length; j += 128) {
            var U = O.slice(j, j + 128);
            $e(U, 64), B = U._R(2), V = br(U, 0, B - T), b.push(V);
            var H = { name: V, type: U._R(1), color: U._R(1), L: U._R(4, "i"), R: U._R(4, "i"), C: U._R(4, "i"), clsid: U._R(16), state: U._R(4, "i"), start: 0, size: 0 }, q = U._R(2) + U._R(2) + U._R(2) + U._R(2);
            q !== 0 && (H.ct = R(U, U.l - 8));
            var pe = U._R(2) + U._R(2) + U._R(2) + U._R(2);
            pe !== 0 && (H.mt = R(U, U.l - 8)), H.start = U._R(4, "i"), H.size = U._R(4, "i"), H.size < 0 && H.start < 0 && (H.size = H.type = 0, H.start = G, H.name = ""), H.type === 5 ? (A = H.start, x > 0 && A !== G && (y[A].name = "!StreamData")) : H.size >= 4096 ? (H.storage = "fat", y[H.start] === void 0 && (y[H.start] = C(v, H.start, y.fat_addrs, y.ssz)), y[H.start].name = H.name, H.content = y[H.start].data.slice(0, H.size)) : (H.storage = "minifat", H.size < 0 ? H.size = 0 : A !== G && H.start !== G && y[A] && (H.content = u(H, y[A].data, (y[P] || {}).data))), H.content && $e(H.content, 0), k[V] = H, D.push(H);
          }
        }
        function R(h, y) {
          return new Date((Ce(h, y + 4) / 1e7 * Math.pow(2, 32) + Ce(h, y) / 1e7 - 11644473600) * 1e3);
        }
        function N(h, y) {
          return f(), l(c.readFileSync(h), y);
        }
        function F(h, y) {
          var v = y && y.type;
          switch (v || "base64") {
            case "file":
              return N(h, y);
            case "base64":
              return l(ht(Qe(h)), y);
            case "binary":
              return l(ht(h), y);
          }
          return l(h, y);
        }
        function S(h, y) {
          var v = y || {}, b = v.root || "Root Entry";
          if (h.FullPaths || (h.FullPaths = []), h.FileIndex || (h.FileIndex = []), h.FullPaths.length !== h.FileIndex.length) throw new Error("inconsistent CFB structure");
          h.FullPaths.length === 0 && (h.FullPaths[0] = b + "/", h.FileIndex[0] = { name: b, type: 5 }), v.CLSID && (h.FileIndex[0].clsid = v.CLSID), W(h);
        }
        function W(h) {
          var y = "Sh33tJ5";
          if (!se.find(h, "/" + y)) {
            var v = Ye(4);
            v[0] = 55, v[1] = v[3] = 50, v[2] = 54, h.FileIndex.push({ name: y, type: 2, content: v, size: 4, L: 69, R: 69, C: 69 }), h.FullPaths.push(h.FullPaths[0] + y), M(h);
          }
        }
        function M(h, y) {
          S(h);
          for (var v = !1, b = !1, x = h.FullPaths.length - 1; x >= 0; --x) {
            var k = h.FileIndex[x];
            switch (k.type) {
              case 0:
                b ? v = !0 : (h.FileIndex.pop(), h.FullPaths.pop());
                break;
              case 1:
              case 2:
              case 5:
                b = !0, isNaN(k.R * k.L * k.C) && (v = !0), k.R > -1 && k.L > -1 && k.R == k.L && (v = !0);
                break;
              default:
                v = !0;
                break;
            }
          }
          if (!(!v && !y)) {
            var D = new Date(1987, 1, 19), P = 0, A = Object.create ? /* @__PURE__ */ Object.create(null) : {}, T = [];
            for (x = 0; x < h.FullPaths.length; ++x)
              A[h.FullPaths[x]] = !0, h.FileIndex[x].type !== 0 && T.push([h.FullPaths[x], h.FileIndex[x]]);
            for (x = 0; x < T.length; ++x) {
              var O = n(T[x][0]);
              b = A[O], b || (T.push([O, { name: a(O).replace("/", ""), type: 1, clsid: ee, ct: D, mt: D, content: null }]), A[O] = !0);
            }
            for (T.sort(function(V, U) {
              return t(V[0], U[0]);
            }), h.FullPaths = [], h.FileIndex = [], x = 0; x < T.length; ++x)
              h.FullPaths[x] = T[x][0], h.FileIndex[x] = T[x][1];
            for (x = 0; x < T.length; ++x) {
              var j = h.FileIndex[x], B = h.FullPaths[x];
              if (j.name = a(B).replace("/", ""), j.L = j.R = j.C = -(j.color = 1), j.size = j.content ? j.content.length : 0, j.start = 0, j.clsid = j.clsid || ee, x === 0)
                j.C = T.length > 1 ? 1 : -1, j.size = 0, j.type = 5;
              else if (B.slice(-1) == "/") {
                for (P = x + 1; P < T.length && n(h.FullPaths[P]) != B; ++P) ;
                for (j.C = P >= T.length ? -1 : P, P = x + 1; P < T.length && n(h.FullPaths[P]) != n(B); ++P) ;
                j.R = P >= T.length ? -1 : P, j.type = 1;
              } else
                n(h.FullPaths[x + 1] || "") == n(B) && (j.R = x + 1), j.type = 2;
            }
          }
        }
        function L(h, y) {
          var v = y || {};
          if (v.fileType == "mad") return Lf(h, v);
          switch (M(h), v.fileType) {
            case "zip":
              return Of(h, v);
          }
          var b = (function(V) {
            for (var U = 0, H = 0, q = 0; q < V.FileIndex.length; ++q) {
              var pe = V.FileIndex[q];
              if (pe.content) {
                var me = pe.content.length;
                me > 0 && (me < 4096 ? U += me + 63 >> 6 : H += me + 511 >> 9);
              }
            }
            for (var Te = V.FullPaths.length + 3 >> 2, nr = U + 7 >> 3, sr = U + 127 >> 7, ir = nr + H + Te + sr, kt = ir + 127 >> 7, Aa = kt <= 109 ? 0 : Math.ceil((kt - 109) / 127); ir + kt + Aa + 127 >> 7 > kt; ) Aa = ++kt <= 109 ? 0 : Math.ceil((kt - 109) / 127);
            var ct = [1, Aa, kt, sr, Te, H, U, 0];
            return V.FileIndex[0].size = U << 6, ct[7] = (V.FileIndex[0].start = ct[0] + ct[1] + ct[2] + ct[3] + ct[4] + ct[5]) + (ct[6] + 7 >> 3), ct;
          })(h), x = Ye(b[7] << 9), k = 0, D = 0;
          {
            for (k = 0; k < 8; ++k) x._W(1, Y[k]);
            for (k = 0; k < 8; ++k) x._W(2, 0);
            for (x._W(2, 62), x._W(2, 3), x._W(2, 65534), x._W(2, 9), x._W(2, 6), k = 0; k < 3; ++k) x._W(2, 0);
            for (x._W(4, 0), x._W(4, b[2]), x._W(4, b[0] + b[1] + b[2] + b[3] - 1), x._W(4, 0), x._W(4, 4096), x._W(4, b[3] ? b[0] + b[1] + b[2] - 1 : G), x._W(4, b[3]), x._W(-4, b[1] ? b[0] - 1 : G), x._W(4, b[1]), k = 0; k < 109; ++k) x._W(-4, k < b[2] ? b[1] + k : -1);
          }
          if (b[1])
            for (D = 0; D < b[1]; ++D) {
              for (; k < 236 + D * 127; ++k) x._W(-4, k < b[2] ? b[1] + k : -1);
              x._W(-4, D === b[1] - 1 ? G : D + 1);
            }
          var P = function(V) {
            for (D += V; k < D - 1; ++k) x._W(-4, k + 1);
            V && (++k, x._W(-4, G));
          };
          for (D = k = 0, D += b[1]; k < D; ++k) x._W(-4, ue.DIFSECT);
          for (D += b[2]; k < D; ++k) x._W(-4, ue.FATSECT);
          P(b[3]), P(b[4]);
          for (var A = 0, T = 0, O = h.FileIndex[0]; A < h.FileIndex.length; ++A)
            O = h.FileIndex[A], O.content && (T = O.content.length, !(T < 4096) && (O.start = D, P(T + 511 >> 9)));
          for (P(b[6] + 7 >> 3); x.l & 511; ) x._W(-4, ue.ENDOFCHAIN);
          for (D = k = 0, A = 0; A < h.FileIndex.length; ++A)
            O = h.FileIndex[A], O.content && (T = O.content.length, !(!T || T >= 4096) && (O.start = D, P(T + 63 >> 6)));
          for (; x.l & 511; ) x._W(-4, ue.ENDOFCHAIN);
          for (k = 0; k < b[4] << 2; ++k) {
            var j = h.FullPaths[k];
            if (!j || j.length === 0) {
              for (A = 0; A < 17; ++A) x._W(4, 0);
              for (A = 0; A < 3; ++A) x._W(4, -1);
              for (A = 0; A < 12; ++A) x._W(4, 0);
              continue;
            }
            O = h.FileIndex[k], k === 0 && (O.start = O.size ? O.start - 1 : G);
            var B = k === 0 && v.root || O.name;
            if (T = 2 * (B.length + 1), x._W(64, B, "utf16le"), x._W(2, T), x._W(1, O.type), x._W(1, O.color), x._W(-4, O.L), x._W(-4, O.R), x._W(-4, O.C), O.clsid) x._W(16, O.clsid, "hex");
            else for (A = 0; A < 4; ++A) x._W(4, 0);
            x._W(4, O.state || 0), x._W(4, 0), x._W(4, 0), x._W(4, 0), x._W(4, 0), x._W(4, O.start), x._W(4, O.size), x._W(4, 0);
          }
          for (k = 1; k < h.FileIndex.length; ++k)
            if (O = h.FileIndex[k], O.size >= 4096) {
              x.l = O.start + 1 << 9;
              {
                for (A = 0; A < O.size; ++A) x._W(1, O.content[A]);
                for (; A & 511; ++A) x._W(1, 0);
              }
            }
          for (k = 1; k < h.FileIndex.length; ++k)
            if (O = h.FileIndex[k], O.size > 0 && O.size < 4096) {
              for (A = 0; A < O.size; ++A) x._W(1, O.content[A]);
              for (; A & 63; ++A) x._W(1, 0);
            }
          for (; x.l < x.length; ) x._W(1, 0);
          return x;
        }
        function I(h, y) {
          var v = h.FullPaths.map(function(A) {
            return A.toUpperCase();
          }), b = v.map(function(A) {
            var T = A.split("/");
            return T[T.length - (A.slice(-1) == "/" ? 2 : 1)];
          }), x = !1;
          y.charCodeAt(0) === 47 ? (x = !0, y = v[0].slice(0, -1) + y) : x = y.indexOf("/") !== -1;
          var k = y.toUpperCase(), D = x === !0 ? v.indexOf(k) : b.indexOf(k);
          if (D !== -1) return h.FileIndex[D];
          var P = !k.match(fr);
          for (k = k.replace(Ut, ""), P && (k = k.replace(fr, "!")), D = 0; D < v.length; ++D)
            if ((P ? v[D].replace(fr, "!") : v[D]).replace(Ut, "") == k || (P ? b[D].replace(fr, "!") : b[D]).replace(Ut, "") == k) return h.FileIndex[D];
          return null;
        }
        var z = 64, G = -2, Q = "d0cf11e0a1b11ae1", Y = [208, 207, 17, 224, 161, 177, 26, 225], ee = "00000000000000000000000000000000", ue = { MAXREGSECT: -6, DIFSECT: -4, FATSECT: -3, ENDOFCHAIN: G, FREESECT: -1, HEADER_SIGNATURE: Q, HEADER_MINOR_VERSION: "3e00", MAXREGSID: -6, NOSTREAM: -1, HEADER_CLSID: ee, EntryTypes: ["unknown", "storage", "stream", "lockbytes", "property", "root"] };
        function st(h, y, v) {
          f();
          var b = L(h, v);
          c.writeFileSync(y, b);
        }
        function ae(h) {
          for (var y = new Array(h.length), v = 0; v < h.length; ++v) y[v] = String.fromCharCode(h[v]);
          return y.join("");
        }
        function te(h, y) {
          var v = L(h, y);
          switch (y && y.type || "buffer") {
            case "file":
              return f(), c.writeFileSync(y.filename, v), v;
            case "binary":
              return typeof v == "string" ? v : ae(v);
            case "base64":
              return Lt(typeof v == "string" ? v : ae(v));
            case "buffer":
            case "array":
              return typeof v == "string" ? ht(v) : v;
          }
          return v;
        }
        var oe;
        function Pe(h) {
          try {
            var y = h.InflateRaw, v = new y();
            if (v._processChunk(new Uint8Array([3, 0]), v._finishFlushFlag), v.bytesRead) oe = h;
            else throw new Error("zlib does not expose bytesRead");
          } catch (b) {
            console.error("cannot use native zlib: " + (b.message || b));
          }
        }
        function je(h, y) {
          if (!oe) return Os(h, y);
          var v = oe.InflateRaw, b = new v(), x = b._processChunk(h.slice(h.l), b._finishFlushFlag);
          return h.l += b.bytesRead, x;
        }
        function Ue(h) {
          return oe ? oe.deflateRawSync(h) : _s(h);
        }
        var Ke = [16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15], Xe = [3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 17, 19, 23, 27, 31, 35, 43, 51, 59, 67, 83, 99, 115, 131, 163, 195, 227, 258], ve = [1, 2, 3, 4, 5, 7, 9, 13, 17, 25, 33, 49, 65, 97, 129, 193, 257, 385, 513, 769, 1025, 1537, 2049, 3073, 4097, 6145, 8193, 12289, 16385, 24577];
        function it(h) {
          var y = (h << 1 | h << 11) & 139536 | (h << 5 | h << 15) & 558144;
          return (y >> 16 | y >> 8 | y) & 255;
        }
        for (var re = typeof Uint8Array < "u", _e = re ? new Uint8Array(256) : [], ot = 0; ot < 256; ++ot) _e[ot] = it(ot);
        function lt(h, y) {
          var v = _e[h & 255];
          return y <= 8 ? v >>> 8 - y : (v = v << 8 | _e[h >> 8 & 255], y <= 16 ? v >>> 16 - y : (v = v << 8 | _e[h >> 16 & 255], v >>> 24 - y));
        }
        function ys(h, y) {
          var v = y & 7, b = y >>> 3;
          return (h[b] | (v <= 6 ? 0 : h[b + 1] << 8)) >>> v & 3;
        }
        function ka(h, y) {
          var v = y & 7, b = y >>> 3;
          return (h[b] | (v <= 5 ? 0 : h[b + 1] << 8)) >>> v & 7;
        }
        function _f(h, y) {
          var v = y & 7, b = y >>> 3;
          return (h[b] | (v <= 4 ? 0 : h[b + 1] << 8)) >>> v & 15;
        }
        function ks(h, y) {
          var v = y & 7, b = y >>> 3;
          return (h[b] | (v <= 3 ? 0 : h[b + 1] << 8)) >>> v & 31;
        }
        function xs(h, y) {
          var v = y & 7, b = y >>> 3;
          return (h[b] | (v <= 1 ? 0 : h[b + 1] << 8)) >>> v & 127;
        }
        function Tr(h, y, v) {
          var b = y & 7, x = y >>> 3, k = (1 << v) - 1, D = h[x] >>> b;
          return v < 8 - b || (D |= h[x + 1] << 8 - b, v < 16 - b) || (D |= h[x + 2] << 16 - b, v < 24 - b) || (D |= h[x + 3] << 24 - b), D & k;
        }
        function Ss(h, y, v) {
          var b = y & 7, x = y >>> 3;
          return b <= 5 ? h[x] |= (v & 7) << b : (h[x] |= v << b & 255, h[x + 1] = (v & 7) >> 8 - b), y + 3;
        }
        function Af(h, y, v) {
          var b = y & 7, x = y >>> 3;
          return v = (v & 1) << b, h[x] |= v, y + 1;
        }
        function Pt(h, y, v) {
          var b = y & 7, x = y >>> 3;
          return v <<= b, h[x] |= v & 255, v >>>= 8, h[x + 1] = v, y + 8;
        }
        function Cs(h, y, v) {
          var b = y & 7, x = y >>> 3;
          return v <<= b, h[x] |= v & 255, v >>>= 8, h[x + 1] = v & 255, h[x + 2] = v >>> 8, y + 16;
        }
        function xa(h, y) {
          var v = h.length, b = 2 * v > y ? 2 * v : y + 5, x = 0;
          if (v >= y) return h;
          if (re) {
            var k = new Uint8Array(b);
            if (k.set) k.set(h);
            else for (; x < v; ++x) k[x] = h[x];
            return k;
          }
          return h.length = b, h;
        }
        function Ze(h) {
          for (var y = new Array(h), v = 0; v < h; ++v) y[v] = 0;
          return y;
        }
        function Er(h, y, v) {
          var b = 1, x = 0, k = 0, D = 0, P = 0, A = h.length, T = re ? new Uint16Array(32) : Ze(32);
          for (k = 0; k < 32; ++k) T[k] = 0;
          for (k = A; k < v; ++k) h[k] = 0;
          A = h.length;
          var O = re ? new Uint16Array(A) : Ze(A);
          for (k = 0; k < A; ++k)
            T[x = h[k]]++, b < x && (b = x), O[k] = 0;
          for (T[0] = 0, k = 1; k <= b; ++k) T[k + 16] = P = P + T[k - 1] << 1;
          for (k = 0; k < A; ++k)
            P = h[k], P != 0 && (O[k] = T[P + 16]++);
          var j = 0;
          for (k = 0; k < A; ++k)
            if (j = h[k], j != 0)
              for (P = lt(O[k], b) >> b - j, D = (1 << b + 4 - j) - 1; D >= 0; --D) y[P | D << j] = j & 15 | k << 4;
          return b;
        }
        var Sa = re ? new Uint16Array(512) : Ze(512), Ca = re ? new Uint16Array(32) : Ze(32);
        if (!re) {
          for (var yt = 0; yt < 512; ++yt) Sa[yt] = 0;
          for (yt = 0; yt < 32; ++yt) Ca[yt] = 0;
        }
        (function() {
          for (var h = [], y = 0; y < 32; y++) h.push(5);
          Er(h, Ca, 32);
          var v = [];
          for (y = 0; y <= 143; y++) v.push(8);
          for (; y <= 255; y++) v.push(9);
          for (; y <= 279; y++) v.push(7);
          for (; y <= 287; y++) v.push(8);
          Er(v, Sa, 288);
        })();
        var Ff = (function() {
          for (var y = re ? new Uint8Array(32768) : [], v = 0, b = 0; v < ve.length - 1; ++v)
            for (; b < ve[v + 1]; ++b) y[b] = v;
          for (; b < 32768; ++b) y[b] = 29;
          var x = re ? new Uint8Array(259) : [];
          for (v = 0, b = 0; v < Xe.length - 1; ++v)
            for (; b < Xe[v + 1]; ++b) x[b] = v;
          function k(P, A) {
            for (var T = 0; T < P.length; ) {
              var O = Math.min(65535, P.length - T), j = T + O == P.length;
              for (A._W(1, +j), A._W(2, O), A._W(2, ~O & 65535); O-- > 0; ) A[A.l++] = P[T++];
            }
            return A.l;
          }
          function D(P, A) {
            for (var T = 0, O = 0, j = re ? new Uint16Array(32768) : []; O < P.length; ) {
              var B = Math.min(65535, P.length - O);
              if (B < 10) {
                for (T = Ss(A, T, +(O + B == P.length)), T & 7 && (T += 8 - (T & 7)), A.l = T / 8 | 0, A._W(2, B), A._W(2, ~B & 65535); B-- > 0; ) A[A.l++] = P[O++];
                T = A.l * 8;
                continue;
              }
              T = Ss(A, T, +(O + B == P.length) + 2);
              for (var V = 0; B-- > 0; ) {
                var U = P[O];
                V = (V << 5 ^ U) & 32767;
                var H = -1, q = 0;
                if ((H = j[V]) && (H |= O & -32768, H > O && (H -= 32768), H < O))
                  for (; P[H + q] == P[O + q] && q < 250; ) ++q;
                if (q > 2) {
                  U = x[q], U <= 22 ? T = Pt(A, T, _e[U + 1] >> 1) - 1 : (Pt(A, T, 3), T += 5, Pt(A, T, _e[U - 23] >> 5), T += 3);
                  var pe = U < 8 ? 0 : U - 4 >> 2;
                  pe > 0 && (Cs(A, T, q - Xe[U]), T += pe), U = y[O - H], T = Pt(A, T, _e[U] >> 3), T -= 3;
                  var me = U < 4 ? 0 : U - 2 >> 1;
                  me > 0 && (Cs(A, T, O - H - ve[U]), T += me);
                  for (var Te = 0; Te < q; ++Te)
                    j[V] = O & 32767, V = (V << 5 ^ P[O]) & 32767, ++O;
                  B -= q - 1;
                } else
                  U <= 143 ? U = U + 48 : T = Af(A, T, 1), T = Pt(A, T, _e[U]), j[V] = O & 32767, ++O;
              }
              T = Pt(A, T, 0) - 1;
            }
            return A.l = (T + 7) / 8 | 0, A.l;
          }
          return function(A, T) {
            return A.length < 8 ? k(A, T) : D(A, T);
          };
        })();
        function _s(h) {
          var y = Ye(50 + Math.floor(h.length * 1.1)), v = Ff(h, y);
          return y.slice(0, v);
        }
        var As = re ? new Uint16Array(32768) : Ze(32768), Fs = re ? new Uint16Array(32768) : Ze(32768), Ts = re ? new Uint16Array(128) : Ze(128), Es = 1, Ds = 1;
        function Tf(h, y) {
          var v = ks(h, y) + 257;
          y += 5;
          var b = ks(h, y) + 1;
          y += 5;
          var x = _f(h, y) + 4;
          y += 4;
          for (var k = 0, D = re ? new Uint8Array(19) : Ze(19), P = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], A = 1, T = re ? new Uint8Array(8) : Ze(8), O = re ? new Uint8Array(8) : Ze(8), j = D.length, B = 0; B < x; ++B)
            D[Ke[B]] = k = ka(h, y), A < k && (A = k), T[k]++, y += 3;
          var V = 0;
          for (T[0] = 0, B = 1; B <= A; ++B) O[B] = V = V + T[B - 1] << 1;
          for (B = 0; B < j; ++B) (V = D[B]) != 0 && (P[B] = O[V]++);
          var U = 0;
          for (B = 0; B < j; ++B)
            if (U = D[B], U != 0) {
              V = _e[P[B]] >> 8 - U;
              for (var H = (1 << 7 - U) - 1; H >= 0; --H) Ts[V | H << U] = U & 7 | B << 3;
            }
          var q = [];
          for (A = 1; q.length < v + b; )
            switch (V = Ts[xs(h, y)], y += V & 7, V >>>= 3) {
              case 16:
                for (k = 3 + ys(h, y), y += 2, V = q[q.length - 1]; k-- > 0; ) q.push(V);
                break;
              case 17:
                for (k = 3 + ka(h, y), y += 3; k-- > 0; ) q.push(0);
                break;
              case 18:
                for (k = 11 + xs(h, y), y += 7; k-- > 0; ) q.push(0);
                break;
              default:
                q.push(V), A < V && (A = V);
                break;
            }
          var pe = q.slice(0, v), me = q.slice(v);
          for (B = v; B < 286; ++B) pe[B] = 0;
          for (B = b; B < 30; ++B) me[B] = 0;
          return Es = Er(pe, As, 286), Ds = Er(me, Fs, 30), y;
        }
        function Ef(h, y) {
          if (h[0] == 3 && !(h[1] & 3))
            return [$t(y), 2];
          for (var v = 0, b = 0, x = js(y || 1 << 18), k = 0, D = x.length >>> 0, P = 0, A = 0; (b & 1) == 0; ) {
            if (b = ka(h, v), v += 3, b >>> 1)
              b >> 1 == 1 ? (P = 9, A = 5) : (v = Tf(h, v), P = Es, A = Ds);
            else {
              v & 7 && (v += 8 - (v & 7));
              var T = h[v >>> 3] | h[(v >>> 3) + 1] << 8;
              if (v += 32, T > 0)
                for (!y && D < k + T && (x = xa(x, k + T), D = x.length); T-- > 0; )
                  x[k++] = h[v >>> 3], v += 8;
              continue;
            }
            for (; ; ) {
              !y && D < k + 32767 && (x = xa(x, k + 32767), D = x.length);
              var O = Tr(h, v, P), j = b >>> 1 == 1 ? Sa[O] : As[O];
              if (v += j & 15, j >>>= 4, (j >>> 8 & 255) === 0) x[k++] = j;
              else {
                if (j == 256) break;
                j -= 257;
                var B = j < 8 ? 0 : j - 4 >> 2;
                B > 5 && (B = 0);
                var V = k + Xe[j];
                B > 0 && (V += Tr(h, v, B), v += B), O = Tr(h, v, A), j = b >>> 1 == 1 ? Ca[O] : Fs[O], v += j & 15, j >>>= 4;
                var U = j < 4 ? 0 : j - 2 >> 1, H = ve[j];
                for (U > 0 && (H += Tr(h, v, U), v += U), !y && D < V && (x = xa(x, V + 100), D = x.length); k < V; )
                  x[k] = x[k - H], ++k;
              }
            }
          }
          return y ? [x, v + 7 >>> 3] : [x.slice(0, k), v + 7 >>> 3];
        }
        function Os(h, y) {
          var v = h.slice(h.l || 0), b = Ef(v, y);
          return h.l += b[1], b[0];
        }
        function Ms(h, y) {
          if (h)
            typeof console < "u" && console.error(y);
          else throw new Error(y);
        }
        function Rs(h, y) {
          var v = h;
          $e(v, 0);
          var b = [], x = [], k = { FileIndex: b, FullPaths: x };
          S(k, { root: y.root });
          for (var D = v.length - 4; (v[D] != 80 || v[D + 1] != 75 || v[D + 2] != 5 || v[D + 3] != 6) && D >= 0; ) --D;
          v.l = D + 4, v.l += 4;
          var P = v._R(2);
          v.l += 6;
          var A = v._R(4);
          for (v.l = A, D = 0; D < P; ++D) {
            v.l += 20;
            var T = v._R(4), O = v._R(4), j = v._R(2), B = v._R(2), V = v._R(2);
            v.l += 8;
            var U = v._R(4), H = o(v.slice(v.l + j, v.l + j + B));
            v.l += j + B + V;
            var q = v.l;
            v.l = U + 4, Df(v, T, O, k, H), v.l = q;
          }
          return k;
        }
        function Df(h, y, v, b, x) {
          h.l += 2;
          var k = h._R(2), D = h._R(2), P = i(h);
          if (k & 8257) throw new Error("Unsupported ZIP encryption");
          for (var A = h._R(4), T = h._R(4), O = h._R(4), j = h._R(2), B = h._R(2), V = "", U = 0; U < j; ++U) V += String.fromCharCode(h[h.l++]);
          if (B) {
            var H = o(h.slice(h.l, h.l + B));
            (H[21589] || {}).mt && (P = H[21589].mt), ((x || {})[21589] || {}).mt && (P = x[21589].mt);
          }
          h.l += B;
          var q = h.slice(h.l, h.l + T);
          switch (D) {
            case 8:
              q = je(h, O);
              break;
            case 0:
              break;
            default:
              throw new Error("Unsupported ZIP Compression method " + D);
          }
          var pe = !1;
          k & 8 && (A = h._R(4), A == 134695760 && (A = h._R(4), pe = !0), T = h._R(4), O = h._R(4)), T != y && Ms(pe, "Bad compressed size: " + y + " != " + T), O != v && Ms(pe, "Bad uncompressed size: " + v + " != " + O), _a(b, V, q, { unsafe: !0, mt: P });
        }
        function Of(h, y) {
          var v = y || {}, b = [], x = [], k = Ye(1), D = v.compression ? 8 : 0, P = 0, A = 0, T = 0, O = 0, j = 0, B = h.FullPaths[0], V = B, U = h.FileIndex[0], H = [], q = 0;
          for (A = 1; A < h.FullPaths.length; ++A)
            if (V = h.FullPaths[A].slice(B.length), U = h.FileIndex[A], !(!U.size || !U.content || V == "Sh33tJ5")) {
              var pe = O, me = Ye(V.length);
              for (T = 0; T < V.length; ++T) me._W(1, V.charCodeAt(T) & 127);
              me = me.slice(0, me.l), H[j] = di.buf(U.content, 0);
              var Te = U.content;
              D == 8 && (Te = Ue(Te)), k = Ye(30), k._W(4, 67324752), k._W(2, 20), k._W(2, P), k._W(2, D), U.mt ? s(k, U.mt) : k._W(4, 0), k._W(-4, H[j]), k._W(4, Te.length), k._W(4, U.content.length), k._W(2, me.length), k._W(2, 0), O += k.length, b.push(k), O += me.length, b.push(me), O += Te.length, b.push(Te), k = Ye(46), k._W(4, 33639248), k._W(2, 0), k._W(2, 20), k._W(2, P), k._W(2, D), k._W(4, 0), k._W(-4, H[j]), k._W(4, Te.length), k._W(4, U.content.length), k._W(2, me.length), k._W(2, 0), k._W(2, 0), k._W(2, 0), k._W(2, 0), k._W(4, 0), k._W(4, pe), q += k.l, x.push(k), q += me.length, x.push(me), ++j;
            }
          return k = Ye(22), k._W(4, 101010256), k._W(2, 0), k._W(2, 0), k._W(2, j), k._W(2, j), k._W(4, q), k._W(4, O), k._W(2, 0), vt([vt(b), vt(x), k]);
        }
        var Dr = { htm: "text/html", xml: "text/xml", gif: "image/gif", jpg: "image/jpeg", png: "image/png", mso: "application/x-mso", thmx: "application/vnd.ms-officetheme", sh33tj5: "application/octet-stream" };
        function Mf(h, y) {
          if (h.ctype) return h.ctype;
          var v = h.name || "", b = v.match(/\.([^\.]+)$/);
          return b && Dr[b[1]] || y && (b = (v = y).match(/[\.\\]([^\.\\])+$/), b && Dr[b[1]]) ? Dr[b[1]] : "application/octet-stream";
        }
        function Rf(h) {
          for (var y = Lt(h), v = [], b = 0; b < y.length; b += 76) v.push(y.slice(b, b + 76));
          return v.join(`\r
`) + `\r
`;
        }
        function Nf(h) {
          var y = h.replace(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7E-\xFF=]/g, function(T) {
            var O = T.charCodeAt(0).toString(16).toUpperCase();
            return "=" + (O.length == 1 ? "0" + O : O);
          });
          y = y.replace(/ $/gm, "=20").replace(/\t$/gm, "=09"), y.charAt(0) == `
` && (y = "=0D" + y.slice(1)), y = y.replace(/\r(?!\n)/gm, "=0D").replace(/\n\n/gm, `
=0A`).replace(/([^\r\n])\n/gm, "$1=0A");
          for (var v = [], b = y.split(`\r
`), x = 0; x < b.length; ++x) {
            var k = b[x];
            if (k.length == 0) {
              v.push("");
              continue;
            }
            for (var D = 0; D < k.length; ) {
              var P = 76, A = k.slice(D, D + P);
              A.charAt(P - 1) == "=" ? P-- : A.charAt(P - 2) == "=" ? P -= 2 : A.charAt(P - 3) == "=" && (P -= 3), A = k.slice(D, D + P), D += P, D < k.length && (A += "="), v.push(A);
            }
          }
          return v.join(`\r
`);
        }
        function Pf(h) {
          for (var y = [], v = 0; v < h.length; ++v) {
            for (var b = h[v]; v <= h.length && b.charAt(b.length - 1) == "="; ) b = b.slice(0, b.length - 1) + h[++v];
            y.push(b);
          }
          for (var x = 0; x < y.length; ++x) y[x] = y[x].replace(/[=][0-9A-Fa-f]{2}/g, function(k) {
            return String.fromCharCode(parseInt(k.slice(1), 16));
          });
          return ht(y.join(`\r
`));
        }
        function Wf(h, y, v) {
          for (var b = "", x = "", k = "", D, P = 0; P < 10; ++P) {
            var A = y[P];
            if (!A || A.match(/^\s*$/)) break;
            var T = A.match(/^(.*?):\s*([^\s].*)$/);
            if (T) switch (T[1].toLowerCase()) {
              case "content-location":
                b = T[2].trim();
                break;
              case "content-type":
                k = T[2].trim();
                break;
              case "content-transfer-encoding":
                x = T[2].trim();
                break;
            }
          }
          switch (++P, x.toLowerCase()) {
            case "base64":
              D = ht(Qe(y.slice(P).join("")));
              break;
            case "quoted-printable":
              D = Pf(y.slice(P));
              break;
            default:
              throw new Error("Unsupported Content-Transfer-Encoding " + x);
          }
          var O = _a(h, b.slice(v.length), D, { unsafe: !0 });
          k && (O.ctype = k);
        }
        function If(h, y) {
          if (ae(h.slice(0, 13)).toLowerCase() != "mime-version:") throw new Error("Unsupported MAD header");
          var v = y && y.root || "", b = ae(h).split(`\r
`), x = 0, k = "";
          for (x = 0; x < b.length; ++x)
            if (k = b[x], !!/^Content-Location:/i.test(k) && (k = k.slice(k.indexOf("file")), v || (v = k.slice(0, k.lastIndexOf("/") + 1)), k.slice(0, v.length) != v))
              for (; v.length > 0 && (v = v.slice(0, v.length - 1), v = v.slice(0, v.lastIndexOf("/") + 1), k.slice(0, v.length) != v); )
                ;
          var D = (b[1] || "").match(/boundary="(.*?)"/);
          if (!D) throw new Error("MAD cannot find boundary");
          var P = "--" + (D[1] || ""), A = [], T = [], O = { FileIndex: A, FullPaths: T };
          S(O);
          var j, B = 0;
          for (x = 0; x < b.length; ++x) {
            var V = b[x];
            V !== P && V !== P + "--" || (B++ && Wf(O, b.slice(j, x), v), j = x);
          }
          return O;
        }
        function Lf(h, y) {
          var v = y || {}, b = v.boundary || "SheetJS";
          b = "------=" + b;
          for (var x = ["MIME-Version: 1.0", 'Content-Type: multipart/related; boundary="' + b.slice(2) + '"', "", "", ""], k = h.FullPaths[0], D = k, P = h.FileIndex[0], A = 1; A < h.FullPaths.length; ++A)
            if (D = h.FullPaths[A].slice(k.length), P = h.FileIndex[A], !(!P.size || !P.content || D == "Sh33tJ5")) {
              D = D.replace(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7E-\xFF]/g, function(q) {
                return "_x" + q.charCodeAt(0).toString(16) + "_";
              }).replace(/[\u0080-\uFFFF]/g, function(q) {
                return "_u" + q.charCodeAt(0).toString(16) + "_";
              });
              for (var T = P.content, O = ae(T), j = 0, B = Math.min(1024, O.length), V = 0, U = 0; U <= B; ++U) (V = O.charCodeAt(U)) >= 32 && V < 128 && ++j;
              var H = j >= B * 4 / 5;
              x.push(b), x.push("Content-Location: " + (v.root || "file:///C:/SheetJS/") + D), x.push("Content-Transfer-Encoding: " + (H ? "quoted-printable" : "base64")), x.push("Content-Type: " + Mf(P, D)), x.push(""), x.push(H ? Nf(O) : Rf(O));
            }
          return x.push(b + `--\r
`), x.join(`\r
`);
        }
        function $f(h) {
          var y = {};
          return S(y, h), y;
        }
        function _a(h, y, v, b) {
          var x = b && b.unsafe;
          x || S(h);
          var k = !x && se.find(h, y);
          if (!k) {
            var D = h.FullPaths[0];
            y.slice(0, D.length) == D ? D = y : (D.slice(-1) != "/" && (D += "/"), D = (D + y).replace("//", "/")), k = { name: a(y), type: 2 }, h.FileIndex.push(k), h.FullPaths.push(D), x || se.utils.cfb_gc(h);
          }
          return k.content = v, k.size = v ? v.length : 0, b && (b.CLSID && (k.clsid = b.CLSID), b.mt && (k.mt = b.mt), b.ct && (k.ct = b.ct)), k;
        }
        function zf(h, y) {
          S(h);
          var v = se.find(h, y);
          if (v) {
            for (var b = 0; b < h.FileIndex.length; ++b) if (h.FileIndex[b] == v)
              return h.FileIndex.splice(b, 1), h.FullPaths.splice(b, 1), !0;
          }
          return !1;
        }
        function jf(h, y, v) {
          S(h);
          var b = se.find(h, y);
          if (b) {
            for (var x = 0; x < h.FileIndex.length; ++x) if (h.FileIndex[x] == b)
              return h.FileIndex[x].name = a(v), h.FullPaths[x] = v, !0;
          }
          return !1;
        }
        function Uf(h) {
          M(h, !0);
        }
        return r.find = I, r.read = F, r.parse = l, r.write = te, r.writeFile = st, r.utils = { cfb_new: $f, cfb_add: _a, cfb_del: zf, cfb_mov: jf, cfb_gc: Uf, ReadShift: Kt, CheckField: yn, prep_blob: $e, bconcat: vt, use_zlib: Pe, _deflateRaw: _s, _inflateRaw: Os, consts: ue }, r;
      })(), mt;
      if (typeof Ns < "u") try {
        mt = void 0;
      } catch {
      }
      function vi(e) {
        return typeof e == "string" ? zt(e) : Array.isArray(e) ? Us(e) : e;
      }
      function Vt(e, r, t) {
        if (typeof mt < "u" && mt.writeFileSync) return t ? mt.writeFileSync(e, r, t) : mt.writeFileSync(e, r);
        if (typeof Deno < "u") {
          if (t && typeof r == "string") switch (t) {
            case "utf8":
              r = new TextEncoder(t).encode(r);
              break;
            case "binary":
              r = zt(r);
              break;
            default:
              throw new Error("Unsupported encoding " + t);
          }
          return Deno.writeFileSync(e, r);
        }
        var n = t == "utf8" ? gt(r) : r;
        if (typeof IE_SaveFile < "u") return IE_SaveFile(n, e);
        if (typeof Blob < "u") {
          var a = new Blob([vi(n)], { type: "application/octet-stream" });
          if (typeof navigator < "u" && navigator.msSaveBlob) return navigator.msSaveBlob(a, e);
          if (typeof saveAs < "u") return saveAs(a, e);
          if (typeof URL < "u" && typeof document < "u" && document.createElement && URL.createObjectURL) {
            var s = URL.createObjectURL(a);
            if (typeof chrome == "object" && typeof (chrome.downloads || {}).download == "function")
              return URL.revokeObjectURL && typeof setTimeout < "u" && setTimeout(function() {
                URL.revokeObjectURL(s);
              }, 6e4), chrome.downloads.download({ url: s, filename: e, saveAs: !0 });
            var i = document.createElement("a");
            if (i.download != null)
              return i.download = e, i.href = s, document.body.appendChild(i), i.click(), document.body.removeChild(i), URL.revokeObjectURL && typeof setTimeout < "u" && setTimeout(function() {
                URL.revokeObjectURL(s);
              }, 6e4), s;
          }
        }
        if (typeof $ < "u" && typeof File < "u" && typeof Folder < "u") try {
          var o = File(e);
          return o.open("w"), o.encoding = "binary", Array.isArray(r) && (r = jt(r)), o.write(r), o.close(), r;
        } catch (c) {
          if (!c.message || !c.message.match(/onstruct/)) throw c;
        }
        throw new Error("cannot save file " + e);
      }
      function gi(e) {
        if (typeof mt < "u") return mt.readFileSync(e);
        if (typeof Deno < "u") return Deno.readFileSync(e);
        if (typeof $ < "u" && typeof File < "u" && typeof Folder < "u") try {
          var r = File(e);
          r.open("r"), r.encoding = "binary";
          var t = r.read();
          return r.close(), t;
        } catch (n) {
          if (!n.message || !n.message.match(/onstruct/)) throw n;
        }
        throw new Error("Cannot access file " + e);
      }
      function De(e) {
        for (var r = Object.keys(e), t = [], n = 0; n < r.length; ++n) Object.prototype.hasOwnProperty.call(e, r[n]) && t.push(r[n]);
        return t;
      }
      function qa(e) {
        for (var r = [], t = De(e), n = 0; n !== t.length; ++n) r[e[t[n]]] = t[n];
        return r;
      }
      function Ja(e) {
        for (var r = [], t = De(e), n = 0; n !== t.length; ++n) r[e[t[n]]] = parseInt(t[n], 10);
        return r;
      }
      function bi(e) {
        for (var r = [], t = De(e), n = 0; n !== t.length; ++n)
          r[e[t[n]]] == null && (r[e[t[n]]] = []), r[e[t[n]]].push(t[n]);
        return r;
      }
      var vr = new Date(1899, 11, 30, 0, 0, 0);
      function Oe(e, r) {
        var t = e.getTime(), n = vr.getTime() + (e.getTimezoneOffset() - vr.getTimezoneOffset()) * 6e4;
        return (t - n) / (1440 * 60 * 1e3);
      }
      var Ka = /* @__PURE__ */ new Date(), wi = vr.getTime() + (Ka.getTimezoneOffset() - vr.getTimezoneOffset()) * 6e4, Za = Ka.getTimezoneOffset();
      function Br(e) {
        var r = /* @__PURE__ */ new Date();
        return r.setTime(e * 24 * 60 * 60 * 1e3 + wi), r.getTimezoneOffset() !== Za && r.setTime(r.getTime() + (r.getTimezoneOffset() - Za) * 6e4), r;
      }
      function yi(e) {
        var r = 0, t = 0, n = !1, a = e.match(/P([0-9\.]+Y)?([0-9\.]+M)?([0-9\.]+D)?T([0-9\.]+H)?([0-9\.]+M)?([0-9\.]+S)?/);
        if (!a) throw new Error("|" + e + "| is not an ISO8601 Duration");
        for (var s = 1; s != a.length; ++s)
          if (a[s]) {
            switch (t = 1, s > 3 && (n = !0), a[s].slice(a[s].length - 1)) {
              case "Y":
                throw new Error("Unsupported ISO Duration Field: " + a[s].slice(a[s].length - 1));
              case "D":
                t *= 24;
              case "H":
                t *= 60;
              case "M":
                if (n) t *= 60;
                else throw new Error("Unsupported ISO Duration Field: M");
            }
            r += t * parseInt(a[s], 10);
          }
        return r;
      }
      var Qa = /* @__PURE__ */ new Date("2017-02-19T19:06:09.000Z"), en = isNaN(Qa.getFullYear()) ? /* @__PURE__ */ new Date("2/19/17") : Qa, ki = en.getFullYear() == 2017;
      function Se(e, r) {
        var t = new Date(e);
        if (ki)
          return r > 0 ? t.setTime(t.getTime() + t.getTimezoneOffset() * 60 * 1e3) : r < 0 && t.setTime(t.getTime() - t.getTimezoneOffset() * 60 * 1e3), t;
        if (e instanceof Date) return e;
        if (en.getFullYear() == 1917 && !isNaN(t.getFullYear())) {
          var n = t.getFullYear();
          return e.indexOf("" + n) > -1 || t.setFullYear(t.getFullYear() + 100), t;
        }
        var a = e.match(/\d+/g) || ["2017", "2", "19", "0", "0", "0"], s = new Date(+a[0], +a[1] - 1, +a[2], +a[3] || 0, +a[4] || 0, +a[5] || 0);
        return e.indexOf("Z") > -1 && (s = new Date(s.getTime() - s.getTimezoneOffset() * 60 * 1e3)), s;
      }
      function _t(e, r) {
        if (typeof TextDecoder < "u") try {
          if (r) {
            if (e[0] == 255 && e[1] == 254) return gt(new TextDecoder("utf-16le").decode(e.slice(2)));
            if (e[0] == 254 && e[1] == 255) return gt(new TextDecoder("utf-16be").decode(e.slice(2)));
          }
          var t = { "€": "", "‚": "", ƒ: "", "„": "", "…": "", "†": "", "‡": "", "ˆ": "", "‰": "", Š: "", "‹": "", Œ: "", Ž: "", "‘": "", "’": "", "“": "", "”": "", "•": "", "–": "", "—": "", "˜": "", "™": "", š: "", "›": "", œ: "", ž: "", Ÿ: "" };
          return Array.isArray(e) && (e = new Uint8Array(e)), new TextDecoder("latin1").decode(e).replace(/[€‚ƒ„…†‡ˆ‰Š‹ŒŽ‘’“”•–—˜™š›œžŸ]/g, function(s) {
            return t[s] || s;
          });
        } catch {
        }
        for (var n = [], a = 0; a != e.length; ++a) n.push(String.fromCharCode(e[a]));
        return n.join("");
      }
      function Ae(e) {
        if (typeof JSON < "u" && !Array.isArray(e)) return JSON.parse(JSON.stringify(e));
        if (typeof e != "object" || e == null) return e;
        if (e instanceof Date) return new Date(e.getTime());
        var r = {};
        for (var t in e) Object.prototype.hasOwnProperty.call(e, t) && (r[t] = Ae(e[t]));
        return r;
      }
      function Re(e, r) {
        for (var t = ""; t.length < r; ) t += e;
        return t;
      }
      function tt(e) {
        var r = Number(e);
        if (!isNaN(r)) return isFinite(r) ? r : NaN;
        if (!/\d/.test(e)) return r;
        var t = 1, n = e.replace(/([\d]),([\d])/g, "$1$2").replace(/[$]/g, "").replace(/[%]/g, function() {
          return t *= 100, "";
        });
        return !isNaN(r = Number(n)) || (n = n.replace(/[(](.*)[)]/, function(a, s) {
          return t = -t, s;
        }), !isNaN(r = Number(n))) ? r / t : r;
      }
      var xi = ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"];
      function Xt(e) {
        var r = new Date(e), t = /* @__PURE__ */ new Date(NaN), n = r.getYear(), a = r.getMonth(), s = r.getDate();
        if (isNaN(s)) return t;
        var i = e.toLowerCase();
        if (i.match(/jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec/)) {
          if (i = i.replace(/[^a-z]/g, "").replace(/([^a-z]|^)[ap]m?([^a-z]|$)/, ""), i.length > 3 && xi.indexOf(i) == -1) return t;
        } else if (i.match(/[a-z]/)) return t;
        return n < 0 || n > 8099 ? t : (a > 0 || s > 1) && n != 101 ? r : e.match(/[^-0-9:,\/\\]/) ? t : r;
      }
      (function() {
        var e = "abacaba".split(/(:?b)/i).length == 5;
        return function(t, n, a) {
          if (e || typeof n == "string") return t.split(n);
          for (var s = t.split(n), i = [s[0]], o = 1; o < s.length; ++o)
            i.push(a), i.push(s[o]);
          return i;
        };
      })();
      function tn(e) {
        return e ? e.content && e.type ? _t(e.content, !0) : e.data ? lr(e.data) : e.asNodeBuffer && Pr ? lr(e.asNodeBuffer().toString("binary")) : e.asBinary ? lr(e.asBinary()) : e._data && e._data.getContent ? lr(_t(Array.prototype.slice.call(e._data.getContent(), 0))) : null : null;
      }
      function rn(e) {
        if (!e) return null;
        if (e.data) return Ta(e.data);
        if (e.asNodeBuffer && Pr) return e.asNodeBuffer();
        if (e._data && e._data.getContent) {
          var r = e._data.getContent();
          return typeof r == "string" ? Ta(r) : Array.prototype.slice.call(r);
        }
        return e.content && e.type ? e.content : null;
      }
      function Si(e) {
        return e && e.name.slice(-4) === ".bin" ? rn(e) : tn(e);
      }
      function Ie(e, r) {
        for (var t = e.FullPaths || De(e.files), n = r.toLowerCase().replace(/[\/]/g, "\\"), a = n.replace(/\\/g, "/"), s = 0; s < t.length; ++s) {
          var i = t[s].replace(/^Root Entry[\/]/, "").toLowerCase();
          if (n == i || a == i) return e.files ? e.files[t[s]] : e.FileIndex[s];
        }
        return null;
      }
      function Hr(e, r) {
        var t = Ie(e, r);
        if (t == null) throw new Error("Cannot find file " + r + " in zip");
        return t;
      }
      function be(e, r, t) {
        if (!t) return Si(Hr(e, r));
        if (!r) return null;
        try {
          return be(e, r);
        } catch {
          return null;
        }
      }
      function Le(e, r, t) {
        if (!t) return tn(Hr(e, r));
        if (!r) return null;
        try {
          return Le(e, r);
        } catch {
          return null;
        }
      }
      function Ci(e, r, t) {
        return rn(Hr(e, r));
      }
      function an(e) {
        for (var r = e.FullPaths || De(e.files), t = [], n = 0; n < r.length; ++n) r[n].slice(-1) != "/" && t.push(r[n].replace(/^Root Entry[\/]/, ""));
        return t.sort();
      }
      function J(e, r, t) {
        if (e.FullPaths) {
          if (typeof t == "string") {
            var n;
            return n = Bs(t), se.utils.cfb_add(e, r, n);
          }
          se.utils.cfb_add(e, r, t);
        } else e.file(r, t);
      }
      function Vr() {
        return se.utils.cfb_new();
      }
      function _i(e, r) {
        switch (r.type) {
          case "base64":
            return se.read(e, { type: "base64" });
          case "binary":
            return se.read(e, { type: "binary" });
          case "buffer":
          case "array":
            return se.read(e, { type: "buffer" });
        }
        throw new Error("Unrecognized type " + r.type);
      }
      function Gt(e, r) {
        if (e.charAt(0) == "/") return e.slice(1);
        var t = r.split("/");
        r.slice(-1) != "/" && t.pop();
        for (var n = e.split("/"); n.length !== 0; ) {
          var a = n.shift();
          a === ".." ? t.pop() : a !== "." && t.push(a);
        }
        return t.join("/");
      }
      var we = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\r
`, Ai = /([^"\s?>\/]+)\s*=\s*((?:")([^"]*)(?:")|(?:')([^']*)(?:')|([^'">\s]+))/g, nn = /<[\/\?]?[a-zA-Z0-9:_-]+(?:\s+[^"\s?>\/]+\s*=\s*(?:"[^"]*"|'[^']*'|[^'">\s=]+))*\s*[\/\?]?>/gm, Fi = /<[^>]*>/g, Ne = we.match(nn) ? nn : Fi, Ti = /<\w*:/, Ei = /<(\/?)\w+:/;
      function K(e, r, t) {
        for (var n = {}, a = 0, s = 0; a !== e.length && !((s = e.charCodeAt(a)) === 32 || s === 10 || s === 13); ++a) ;
        if (r || (n[0] = e.slice(0, a)), a === e.length) return n;
        var i = e.match(Ai), o = 0, c = "", f = 0, l = "", p = "", d = 1;
        if (i) for (f = 0; f != i.length; ++f) {
          for (p = i[f], s = 0; s != p.length && p.charCodeAt(s) !== 61; ++s) ;
          for (l = p.slice(0, s).trim(); p.charCodeAt(s + 1) == 32; ) ++s;
          for (d = (a = p.charCodeAt(s + 1)) == 34 || a == 39 ? 1 : 0, c = p.slice(s + 1 + d, p.length - d), o = 0; o != l.length && l.charCodeAt(o) !== 58; ++o) ;
          if (o === l.length)
            l.indexOf("_") > 0 && (l = l.slice(0, l.indexOf("_"))), n[l] = c, n[l.toLowerCase()] = c;
          else {
            var m = (o === 5 && l.slice(0, 5) === "xmlns" ? "xmlns" : "") + l.slice(o + 1);
            if (n[m] && l.slice(o - 3, o) == "ext") continue;
            n[m] = c, n[m.toLowerCase()] = c;
          }
        }
        return n;
      }
      function rt(e) {
        return e.replace(Ei, "<$1");
      }
      var sn = { "&quot;": '"', "&apos;": "'", "&gt;": ">", "&lt;": "<", "&amp;": "&" }, on = qa(sn), fe = /* @__PURE__ */ (function() {
        var e = /&(?:quot|apos|gt|lt|amp|#x?([\da-fA-F]+));/gi, r = /_x([\da-fA-F]{4})_/gi;
        return function t(n) {
          var a = n + "", s = a.indexOf("<![CDATA[");
          if (s == -1) return a.replace(e, function(o, c) {
            return sn[o] || String.fromCharCode(parseInt(c, o.indexOf("x") > -1 ? 16 : 10)) || o;
          }).replace(r, function(o, c) {
            return String.fromCharCode(parseInt(c, 16));
          });
          var i = a.indexOf("]]>");
          return t(a.slice(0, s)) + a.slice(s + 9, i) + t(a.slice(i + 3));
        };
      })(), ln = /[&<>'"]/g, Di = /[\u0000-\u0008\u000b-\u001f]/g;
      function he(e) {
        var r = e + "";
        return r.replace(ln, function(t) {
          return on[t];
        }).replace(Di, function(t) {
          return "_x" + ("000" + t.charCodeAt(0).toString(16)).slice(-4) + "_";
        });
      }
      var Oi = /[\u0000-\u001f]/g;
      function Xr(e) {
        var r = e + "";
        return r.replace(ln, function(t) {
          return on[t];
        }).replace(/\n/g, "<br/>").replace(Oi, function(t) {
          return "&#x" + ("000" + t.charCodeAt(0).toString(16)).slice(-4) + ";";
        });
      }
      function de(e) {
        switch (e) {
          case 1:
          case !0:
          case "1":
          case "true":
          case "TRUE":
            return !0;
          default:
            return !1;
        }
      }
      function Mi(e) {
        for (var r = "", t = 0, n = 0, a = 0, s = 0, i = 0, o = 0; t < e.length; ) {
          if (n = e.charCodeAt(t++), n < 128) {
            r += String.fromCharCode(n);
            continue;
          }
          if (a = e.charCodeAt(t++), n > 191 && n < 224) {
            i = (n & 31) << 6, i |= a & 63, r += String.fromCharCode(i);
            continue;
          }
          if (s = e.charCodeAt(t++), n < 240) {
            r += String.fromCharCode((n & 15) << 12 | (a & 63) << 6 | s & 63);
            continue;
          }
          i = e.charCodeAt(t++), o = ((n & 7) << 18 | (a & 63) << 12 | (s & 63) << 6 | i & 63) - 65536, r += String.fromCharCode(55296 + (o >>> 10 & 1023)), r += String.fromCharCode(56320 + (o & 1023));
        }
        return r;
      }
      var ie = Mi, gt = function(e) {
        for (var r = [], t = 0, n = 0, a = 0; t < e.length; )
          switch (n = e.charCodeAt(t++), !0) {
            case n < 128:
              r.push(String.fromCharCode(n));
              break;
            case n < 2048:
              r.push(String.fromCharCode(192 + (n >> 6))), r.push(String.fromCharCode(128 + (n & 63)));
              break;
            case (n >= 55296 && n < 57344):
              n -= 55296, a = e.charCodeAt(t++) - 56320 + (n << 10), r.push(String.fromCharCode(240 + (a >> 18 & 7))), r.push(String.fromCharCode(144 + (a >> 12 & 63))), r.push(String.fromCharCode(128 + (a >> 6 & 63))), r.push(String.fromCharCode(128 + (a & 63)));
              break;
            default:
              r.push(String.fromCharCode(224 + (n >> 12))), r.push(String.fromCharCode(128 + (n >> 6 & 63))), r.push(String.fromCharCode(128 + (n & 63)));
          }
        return r.join("");
      }, Yt = /* @__PURE__ */ (function() {
        var e = {};
        return function(t, n) {
          var a = t + "|" + (n || "");
          return e[a] ? e[a] : e[a] = new RegExp("<(?:\\w+:)?" + t + '(?: xml:space="preserve")?(?:[^>]*)>([\\s\\S]*?)</(?:\\w+:)?' + t + ">", n || "");
        };
      })(), Ri = (function() {
        var e = [["nbsp", " "], ["middot", "·"], ["quot", '"'], ["apos", "'"], ["gt", ">"], ["lt", "<"], ["amp", "&"]].map(function(r) {
          return [new RegExp("&" + r[0] + ";", "ig"), r[1]];
        });
        return function(t) {
          for (var n = t.replace(/^[\t\n\r ]+/, "").replace(/[\t\n\r ]+$/, "").replace(/>\s+/g, ">").replace(/\s+</g, "<").replace(/[\t\n\r ]+/g, " ").replace(/<\s*[bB][rR]\s*\/?>/g, `
`).replace(/<[^>]*>/g, ""), a = 0; a < e.length; ++a) n = n.replace(e[a][0], e[a][1]);
          return n;
        };
      })(), Ni = /* @__PURE__ */ (function() {
        var e = {};
        return function(t) {
          return e[t] !== void 0 ? e[t] : e[t] = new RegExp("<(?:vt:)?" + t + ">([\\s\\S]*?)</(?:vt:)?" + t + ">", "g");
        };
      })(), Pi = /<\/?(?:vt:)?variant>/g, Wi = /<(?:vt:)([^>]*)>([\s\S]*)</;
      function cn(e, r) {
        var t = K(e), n = e.match(Ni(t.baseType)) || [], a = [];
        if (n.length != t.size) {
          if (r.WTF) throw new Error("unexpected vector length " + n.length + " != " + t.size);
          return a;
        }
        return n.forEach(function(s) {
          var i = s.replace(Pi, "").match(Wi);
          i && a.push({ v: ie(i[2]), t: i[1] });
        }), a;
      }
      var fn = /(^\s|\s$|\n)/;
      function Ve(e, r) {
        return "<" + e + (r.match(fn) ? ' xml:space="preserve"' : "") + ">" + r + "</" + e + ">";
      }
      function qt(e) {
        return De(e).map(function(r) {
          return " " + r + '="' + e[r] + '"';
        }).join("");
      }
      function X(e, r, t) {
        return "<" + e + (t != null ? qt(t) : "") + (r != null ? (r.match(fn) ? ' xml:space="preserve"' : "") + ">" + r + "</" + e : "/") + ">";
      }
      function Gr(e, r) {
        try {
          return e.toISOString().replace(/\.\d*/, "");
        } catch (t) {
          if (r) throw t;
        }
        return "";
      }
      function Ii(e, r) {
        switch (typeof e) {
          case "string":
            var t = X("vt:lpwstr", he(e));
            return t = t.replace(/&quot;/g, "_x0022_"), t;
          case "number":
            return X((e | 0) == e ? "vt:i4" : "vt:r8", he(String(e)));
          case "boolean":
            return X("vt:bool", e ? "true" : "false");
        }
        if (e instanceof Date) return X("vt:filetime", Gr(e));
        throw new Error("Unable to serialize " + e);
      }
      function un(e) {
        if (typeof e == "string") return e;
        if (typeof Uint8Array < "u" && e instanceof Uint8Array) return ie(jt(Wr(e)));
        throw new Error("Bad input format: expected Buffer or string");
      }
      var Yr = /<(\/?)([^\s?><!\/:]*:|)([^\s?<>:\/]+)(?:[\s?:\/][^>]*)?>/gm, xe = { CORE_PROPS: "http://schemas.openxmlformats.org/package/2006/metadata/core-properties", CUST_PROPS: "http://schemas.openxmlformats.org/officeDocument/2006/custom-properties", EXT_PROPS: "http://schemas.openxmlformats.org/officeDocument/2006/extended-properties", CT: "http://schemas.openxmlformats.org/package/2006/content-types", RELS: "http://schemas.openxmlformats.org/package/2006/relationships", TCMNT: "http://schemas.microsoft.com/office/spreadsheetml/2018/threadedcomments", dc: "http://purl.org/dc/elements/1.1/", dcterms: "http://purl.org/dc/terms/", dcmitype: "http://purl.org/dc/dcmitype/", r: "http://schemas.openxmlformats.org/officeDocument/2006/relationships", vt: "http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes", xsi: "http://www.w3.org/2001/XMLSchema-instance", xsd: "http://www.w3.org/2001/XMLSchema" }, bt = ["http://schemas.openxmlformats.org/spreadsheetml/2006/main", "http://purl.oclc.org/ooxml/spreadsheetml/main", "http://schemas.microsoft.com/office/excel/2006/main", "http://schemas.microsoft.com/office/excel/2006/2"], gr = { o: "urn:schemas-microsoft-com:office:office", x: "urn:schemas-microsoft-com:office:excel", mv: "http://macVmlSchemaUri", v: "urn:schemas-microsoft-com:vml" };
      function Li(e, r) {
        for (var t = 1 - 2 * (e[r + 7] >>> 7), n = ((e[r + 7] & 127) << 4) + (e[r + 6] >>> 4 & 15), a = e[r + 6] & 15, s = 5; s >= 0; --s) a = a * 256 + e[r + s];
        return n == 2047 ? a == 0 ? t * (1 / 0) : NaN : (n == 0 ? n = -1022 : (n -= 1023, a += Math.pow(2, 52)), t * Math.pow(2, n - 52) * a);
      }
      function $i(e, r, t) {
        var n = (r < 0 || 1 / r == -1 / 0 ? 1 : 0) << 7, a = 0, s = 0, i = n ? -r : r;
        isFinite(i) ? i == 0 ? a = s = 0 : (a = Math.floor(Math.log(i) / Math.LN2), s = i * Math.pow(2, 52 - a), a <= -1023 && (!isFinite(s) || s < Math.pow(2, 52)) ? a = -1022 : (s -= Math.pow(2, 52), a += 1023)) : (a = 2047, s = isNaN(r) ? 26985 : 0);
        for (var o = 0; o <= 5; ++o, s /= 256) e[t + o] = s & 255;
        e[t + 6] = (a & 15) << 4 | s & 15, e[t + 7] = a >> 4 | n;
      }
      var zi = function(e) {
        for (var r = [], t = 10240, n = 0; n < e[0].length; ++n) if (e[0][n]) for (var a = 0, s = e[0][n].length; a < s; a += t) r.push.apply(r, e[0][n].slice(a, a + t));
        return r;
      }, hn = zi, ji = function(e, r, t) {
        for (var n = [], a = r; a < t; a += 2) n.push(String.fromCharCode(Jt(e, a)));
        return n.join("").replace(Ut, "");
      }, br = ji, Ui = function(e, r, t) {
        for (var n = [], a = r; a < r + t; ++a) n.push(("0" + e[a].toString(16)).slice(-2));
        return n.join("");
      }, pn = Ui, Bi = function(e, r, t) {
        for (var n = [], a = r; a < t; a++) n.push(String.fromCharCode(Ft(e, a)));
        return n.join("");
      }, At = Bi, Hi = function(e, r) {
        var t = Ce(e, r);
        return t > 0 ? At(e, r + 4, r + 4 + t - 1) : "";
      }, mn = Hi, Vi = function(e, r) {
        var t = Ce(e, r);
        return t > 0 ? At(e, r + 4, r + 4 + t - 1) : "";
      }, dn = Vi, Xi = function(e, r) {
        var t = 2 * Ce(e, r);
        return t > 0 ? At(e, r + 4, r + 4 + t - 1) : "";
      }, vn = Xi, Gi = function(r, t) {
        var n = Ce(r, t);
        return n > 0 ? br(r, t + 4, t + 4 + n) : "";
      }, gn = Gi, Yi = function(e, r) {
        var t = Ce(e, r);
        return t > 0 ? At(e, r + 4, r + 4 + t) : "";
      }, bn = Yi, qi = function(e, r) {
        return Li(e, r);
      }, wn = qi;
      function Ji() {
        br = function(e, r, t) {
          return $cptable.utils.decode(1200, e.slice(r, t)).replace(Ut, "");
        }, At = function(e, r, t) {
          return $cptable.utils.decode(65001, e.slice(r, t));
        }, mn = function(e, r) {
          var t = Ce(e, r);
          return t > 0 ? $cptable.utils.decode(It, e.slice(r + 4, r + 4 + t - 1)) : "";
        }, dn = function(e, r) {
          var t = Ce(e, r);
          return t > 0 ? $cptable.utils.decode(ft, e.slice(r + 4, r + 4 + t - 1)) : "";
        }, vn = function(e, r) {
          var t = 2 * Ce(e, r);
          return t > 0 ? $cptable.utils.decode(1200, e.slice(r + 4, r + 4 + t - 1)) : "";
        }, gn = function(e, r) {
          var t = Ce(e, r);
          return t > 0 ? $cptable.utils.decode(1200, e.slice(r + 4, r + 4 + t)) : "";
        }, bn = function(e, r) {
          var t = Ce(e, r);
          return t > 0 ? $cptable.utils.decode(65001, e.slice(r + 4, r + 4 + t)) : "";
        };
      }
      typeof $cptable < "u" && Ji();
      var Ft = function(e, r) {
        return e[r];
      }, Jt = function(e, r) {
        return e[r + 1] * 256 + e[r];
      }, Ki = function(e, r) {
        var t = e[r + 1] * 256 + e[r];
        return t < 32768 ? t : (65535 - t + 1) * -1;
      }, Ce = function(e, r) {
        return e[r + 3] * (1 << 24) + (e[r + 2] << 16) + (e[r + 1] << 8) + e[r];
      }, Tt = function(e, r) {
        return e[r + 3] << 24 | e[r + 2] << 16 | e[r + 1] << 8 | e[r];
      }, Zi = function(e, r) {
        return e[r] << 24 | e[r + 1] << 16 | e[r + 2] << 8 | e[r + 3];
      };
      function Kt(e, r) {
        var t = "", n, a, s = [], i, o, c, f;
        switch (r) {
          case "dbcs":
            for (f = this.l, c = 0; c < e; ++c)
              t += String.fromCharCode(Jt(this, f)), f += 2;
            e *= 2;
            break;
          case "utf8":
            t = At(this, this.l, this.l + e);
            break;
          case "utf16le":
            e *= 2, t = br(this, this.l, this.l + e);
            break;
          case "wstr":
            if (typeof $cptable < "u") t = $cptable.utils.decode(ft, this.slice(this.l, this.l + 2 * e));
            else return Kt.call(this, e, "dbcs");
            e = 2 * e;
            break;
          case "lpstr-ansi":
            t = mn(this, this.l), e = 4 + Ce(this, this.l);
            break;
          case "lpstr-cp":
            t = dn(this, this.l), e = 4 + Ce(this, this.l);
            break;
          case "lpwstr":
            t = vn(this, this.l), e = 4 + 2 * Ce(this, this.l);
            break;
          case "lpp4":
            e = 4 + Ce(this, this.l), t = gn(this, this.l), e & 2 && (e += 2);
            break;
          case "8lpp4":
            e = 4 + Ce(this, this.l), t = bn(this, this.l), e & 3 && (e += 4 - (e & 3));
            break;
          case "cstr":
            for (e = 0, t = ""; (i = Ft(this, this.l + e++)) !== 0; ) s.push(cr(i));
            t = s.join("");
            break;
          case "_wstr":
            for (e = 0, t = ""; (i = Jt(this, this.l + e)) !== 0; )
              s.push(cr(i)), e += 2;
            e += 2, t = s.join("");
            break;
          case "dbcs-cont":
            for (t = "", f = this.l, c = 0; c < e; ++c) {
              if (this.lens && this.lens.indexOf(f) !== -1)
                return i = Ft(this, f), this.l = f + 1, o = Kt.call(this, e - c, i ? "dbcs-cont" : "sbcs-cont"), s.join("") + o;
              s.push(cr(Jt(this, f))), f += 2;
            }
            t = s.join(""), e *= 2;
            break;
          case "cpstr":
            if (typeof $cptable < "u") {
              t = $cptable.utils.decode(ft, this.slice(this.l, this.l + e));
              break;
            }
          case "sbcs-cont":
            for (t = "", f = this.l, c = 0; c != e; ++c) {
              if (this.lens && this.lens.indexOf(f) !== -1)
                return i = Ft(this, f), this.l = f + 1, o = Kt.call(this, e - c, i ? "dbcs-cont" : "sbcs-cont"), s.join("") + o;
              s.push(cr(Ft(this, f))), f += 1;
            }
            t = s.join("");
            break;
          default:
            switch (e) {
              case 1:
                return n = Ft(this, this.l), this.l++, n;
              case 2:
                return n = (r === "i" ? Ki : Jt)(this, this.l), this.l += 2, n;
              case 4:
              case -4:
                return r === "i" || (this[this.l + 3] & 128) === 0 ? (n = (e > 0 ? Tt : Zi)(this, this.l), this.l += 4, n) : (a = Ce(this, this.l), this.l += 4, a);
              case 8:
              case -8:
                if (r === "f")
                  return e == 8 ? a = wn(this, this.l) : a = wn([this[this.l + 7], this[this.l + 6], this[this.l + 5], this[this.l + 4], this[this.l + 3], this[this.l + 2], this[this.l + 1], this[this.l + 0]], 0), this.l += 8, a;
                e = 8;
              case 16:
                t = pn(this, this.l, e);
                break;
            }
        }
        return this.l += e, t;
      }
      var Qi = function(e, r, t) {
        e[t] = r & 255, e[t + 1] = r >>> 8 & 255, e[t + 2] = r >>> 16 & 255, e[t + 3] = r >>> 24 & 255;
      }, eo = function(e, r, t) {
        e[t] = r & 255, e[t + 1] = r >> 8 & 255, e[t + 2] = r >> 16 & 255, e[t + 3] = r >> 24 & 255;
      }, to = function(e, r, t) {
        e[t] = r & 255, e[t + 1] = r >>> 8 & 255;
      };
      function ro(e, r, t) {
        var n = 0, a = 0;
        if (t === "dbcs") {
          for (a = 0; a != r.length; ++a) to(this, r.charCodeAt(a), this.l + 2 * a);
          n = 2 * r.length;
        } else if (t === "sbcs") {
          if (typeof $cptable < "u" && It == 874)
            for (a = 0; a != r.length; ++a) {
              var s = $cptable.utils.encode(It, r.charAt(a));
              this[this.l + a] = s[0];
            }
          else
            for (r = r.replace(/[^\x00-\x7F]/g, "_"), a = 0; a != r.length; ++a) this[this.l + a] = r.charCodeAt(a) & 255;
          n = r.length;
        } else if (t === "hex") {
          for (; a < e; ++a)
            this[this.l++] = parseInt(r.slice(2 * a, 2 * a + 2), 16) || 0;
          return this;
        } else if (t === "utf16le") {
          var i = Math.min(this.l + e, this.length);
          for (a = 0; a < Math.min(r.length, e); ++a) {
            var o = r.charCodeAt(a);
            this[this.l++] = o & 255, this[this.l++] = o >> 8;
          }
          for (; this.l < i; ) this[this.l++] = 0;
          return this;
        } else switch (e) {
          case 1:
            n = 1, this[this.l] = r & 255;
            break;
          case 2:
            n = 2, this[this.l] = r & 255, r >>>= 8, this[this.l + 1] = r & 255;
            break;
          case 3:
            n = 3, this[this.l] = r & 255, r >>>= 8, this[this.l + 1] = r & 255, r >>>= 8, this[this.l + 2] = r & 255;
            break;
          case 4:
            n = 4, Qi(this, r, this.l);
            break;
          case 8:
            if (n = 8, t === "f") {
              $i(this, r, this.l);
              break;
            }
          case 16:
            break;
          case -4:
            n = 4, eo(this, r, this.l);
            break;
        }
        return this.l += n, this;
      }
      function yn(e, r) {
        var t = pn(this, this.l, e.length >> 1);
        if (t !== e) throw new Error(r + "Expected " + e + " saw " + t);
        this.l += e.length >> 1;
      }
      function $e(e, r) {
        e.l = r, e._R = Kt, e.chk = yn, e._W = ro;
      }
      function Ye(e) {
        var r = $t(e);
        return $e(r, 0), r;
      }
      function ao(e, r, t) {
        if (e) {
          var n, a, s;
          $e(e, e.l || 0);
          for (var i = e.length, o = 0, c = 0; e.l < i; ) {
            o = e._R(1), o & 128 && (o = (o & 127) + ((e._R(1) & 127) << 7));
            var f = XLSBRecordEnum[o] || XLSBRecordEnum[65535];
            for (n = e._R(1), s = n & 127, a = 1; a < 4 && n & 128; ++a) s += ((n = e._R(1)) & 127) << 7 * a;
            c = e.l + s;
            var l = f.f && f.f(e, s, t);
            if (e.l = c, r(l, f, o)) return;
          }
        }
      }
      function no() {
        var e = [], r = 2048, t = function(f) {
          var l = Ye(f);
          return $e(l, 0), l;
        }, n = t(r), a = function() {
          n && (n.length > n.l && (n = n.slice(0, n.l), n.l = n.length), n.length > 0 && e.push(n), n = null);
        }, s = function(f) {
          return n && f < n.length - n.l ? n : (a(), n = t(Math.max(f + 1, r)));
        }, i = function() {
          return a(), vt(e);
        }, o = function(f) {
          a(), n = f, n.l == null && (n.l = n.length), s(r);
        };
        return { next: s, push: o, end: i, _bufs: e };
      }
      function qr(e) {
        return parseInt(so(e), 10) - 1;
      }
      function qe(e) {
        return "" + (e + 1);
      }
      function so(e) {
        return e.replace(/\$(\d+)$/, "$1");
      }
      function Jr(e) {
        for (var r = io(e), t = 0, n = 0; n !== r.length; ++n) t = 26 * t + r.charCodeAt(n) - 64;
        return t - 1;
      }
      function at(e) {
        if (e < 0) throw new Error("invalid column " + e);
        var r = "";
        for (++e; e; e = Math.floor((e - 1) / 26)) r = String.fromCharCode((e - 1) % 26 + 65) + r;
        return r;
      }
      function io(e) {
        return e.replace(/^\$([A-Z])/, "$1");
      }
      function oo(e) {
        return e.replace(/(\$?[A-Z]*)(\$?\d*)/, "$1,$2").split(",");
      }
      function Fe(e) {
        for (var r = 0, t = 0, n = 0; n < e.length; ++n) {
          var a = e.charCodeAt(n);
          a >= 48 && a <= 57 ? r = 10 * r + (a - 48) : a >= 65 && a <= 90 && (t = 26 * t + (a - 64));
        }
        return { c: t - 1, r: r - 1 };
      }
      function ge(e) {
        for (var r = e.c + 1, t = ""; r; r = (r - 1) / 26 | 0) t = String.fromCharCode((r - 1) % 26 + 65) + t;
        return t + (e.r + 1);
      }
      function nt(e) {
        var r = e.indexOf(":");
        return r == -1 ? { s: Fe(e), e: Fe(e) } : { s: Fe(e.slice(0, r)), e: Fe(e.slice(r + 1)) };
      }
      function ye(e, r) {
        return typeof r > "u" || typeof r == "number" ? ye(e.s, e.e) : (typeof e != "string" && (e = ge(e)), typeof r != "string" && (r = ge(r)), e == r ? e : e + ":" + r);
      }
      function ke(e) {
        var r = { s: { c: 0, r: 0 }, e: { c: 0, r: 0 } }, t = 0, n = 0, a = 0, s = e.length;
        for (t = 0; n < s && !((a = e.charCodeAt(n) - 64) < 1 || a > 26); ++n)
          t = 26 * t + a;
        for (r.s.c = --t, t = 0; n < s && !((a = e.charCodeAt(n) - 48) < 0 || a > 9); ++n)
          t = 10 * t + a;
        if (r.s.r = --t, n === s || a != 10)
          return r.e.c = r.s.c, r.e.r = r.s.r, r;
        for (++n, t = 0; n != s && !((a = e.charCodeAt(n) - 64) < 1 || a > 26); ++n)
          t = 26 * t + a;
        for (r.e.c = --t, t = 0; n != s && !((a = e.charCodeAt(n) - 48) < 0 || a > 9); ++n)
          t = 10 * t + a;
        return r.e.r = --t, r;
      }
      function kn(e, r) {
        var t = e.t == "d" && r instanceof Date;
        if (e.z != null) try {
          return e.w = He(e.z, t ? Oe(r) : r);
        } catch {
        }
        try {
          return e.w = He((e.XF || {}).numFmtId || (t ? 14 : 0), t ? Oe(r) : r);
        } catch {
          return "" + r;
        }
      }
      function Et(e, r, t) {
        return e == null || e.t == null || e.t == "z" ? "" : e.w !== void 0 ? e.w : (e.t == "d" && !e.z && t && t.dateNF && (e.z = t.dateNF), e.t == "e" ? Zr[e.v] || e.v : r == null ? kn(e, e.v) : kn(e, r));
      }
      function Dt(e, r) {
        var t = r && r.sheet ? r.sheet : "Sheet1", n = {};
        return n[t] = e, { SheetNames: [t], Sheets: n };
      }
      function xn(e, r, t) {
        var n = t || {}, a = e ? Array.isArray(e) : n.dense, s = e || (a ? [] : {}), i = 0, o = 0;
        if (s && n.origin != null) {
          if (typeof n.origin == "number") i = n.origin;
          else {
            var c = typeof n.origin == "string" ? Fe(n.origin) : n.origin;
            i = c.r, o = c.c;
          }
          s["!ref"] || (s["!ref"] = "A1:A1");
        }
        var f = { s: { c: 1e7, r: 1e7 }, e: { c: 0, r: 0 } };
        if (s["!ref"]) {
          var l = ke(s["!ref"]);
          f.s.c = l.s.c, f.s.r = l.s.r, f.e.c = Math.max(f.e.c, l.e.c), f.e.r = Math.max(f.e.r, l.e.r), i == -1 && (f.e.r = i = l.e.r + 1);
        }
        for (var p = 0; p != r.length; ++p)
          if (r[p]) {
            if (!Array.isArray(r[p])) throw new Error("aoa_to_sheet expects an array of arrays");
            for (var d = 0; d != r[p].length; ++d)
              if (!(typeof r[p][d] > "u")) {
                var m = { v: r[p][d] }, w = i + p, u = o + d;
                if (f.s.r > w && (f.s.r = w), f.s.c > u && (f.s.c = u), f.e.r < w && (f.e.r = w), f.e.c < u && (f.e.c = u), r[p][d] && typeof r[p][d] == "object" && !Array.isArray(r[p][d]) && !(r[p][d] instanceof Date)) m = r[p][d];
                else if (Array.isArray(m.v) && (m.f = r[p][d][1], m.v = m.v[0]), m.v === null)
                  if (m.f) m.t = "n";
                  else if (n.nullError)
                    m.t = "e", m.v = 0;
                  else if (n.sheetStubs) m.t = "z";
                  else continue;
                else typeof m.v == "number" ? m.t = "n" : typeof m.v == "boolean" ? m.t = "b" : m.v instanceof Date ? (m.z = n.dateNF || ce[14], n.cellDates ? (m.t = "d", m.w = He(m.z, Oe(m.v))) : (m.t = "n", m.v = Oe(m.v), m.w = He(m.z, m.v))) : m.t = "s";
                if (a)
                  s[w] || (s[w] = []), s[w][u] && s[w][u].z && (m.z = s[w][u].z), s[w][u] = m;
                else {
                  var g = ge({ c: u, r: w });
                  s[g] && s[g].z && (m.z = s[g].z), s[g] = m;
                }
              }
          }
        return f.s.c < 1e7 && (s["!ref"] = ye(f)), s;
      }
      function Ot(e, r) {
        return xn(null, e, r);
      }
      function lo(e) {
        return e.map(function(r) {
          return [r >> 16 & 255, r >> 8 & 255, r & 255];
        });
      }
      var co = lo([0, 16777215, 16711680, 65280, 255, 16776960, 16711935, 65535, 0, 16777215, 16711680, 65280, 255, 16776960, 16711935, 65535, 8388608, 32768, 128, 8421376, 8388736, 32896, 12632256, 8421504, 10066431, 10040166, 16777164, 13434879, 6684774, 16744576, 26316, 13421823, 128, 16711935, 16776960, 65535, 8388736, 8388608, 32896, 255, 52479, 13434879, 13434828, 16777113, 10079487, 16751052, 13408767, 16764057, 3368703, 3394764, 10079232, 16763904, 16750848, 16737792, 6710937, 9868950, 13158, 3381606, 13056, 3355392, 10040064, 10040166, 3355545, 3355443, 16777215, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]), Kr = Ae(co), Zr = { 0: "#NULL!", 7: "#DIV/0!", 15: "#VALUE!", 23: "#REF!", 29: "#NAME?", 36: "#NUM!", 42: "#N/A", 43: "#GETTING_DATA", 255: "#WTF?" }, fo = { "#NULL!": 0, "#DIV/0!": 7, "#VALUE!": 15, "#REF!": 23, "#NAME?": 29, "#NUM!": 36, "#N/A": 42, "#GETTING_DATA": 43, "#WTF?": 255 }, Qr = {
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml": "workbooks",
        "application/vnd.ms-excel.sheet.macroEnabled.main+xml": "workbooks",
        "application/vnd.ms-excel.sheet.binary.macroEnabled.main": "workbooks",
        "application/vnd.ms-excel.addin.macroEnabled.main+xml": "workbooks",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.template.main+xml": "workbooks",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml": "sheets",
        "application/vnd.ms-excel.worksheet": "sheets",
        "application/vnd.ms-excel.binIndexWs": "TODO",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.chartsheet+xml": "charts",
        "application/vnd.ms-excel.chartsheet": "charts",
        "application/vnd.ms-excel.macrosheet+xml": "macros",
        "application/vnd.ms-excel.macrosheet": "macros",
        "application/vnd.ms-excel.intlmacrosheet": "TODO",
        "application/vnd.ms-excel.binIndexMs": "TODO",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.dialogsheet+xml": "dialogs",
        "application/vnd.ms-excel.dialogsheet": "dialogs",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml": "strs",
        "application/vnd.ms-excel.sharedStrings": "strs",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml": "styles",
        "application/vnd.ms-excel.styles": "styles",
        "application/vnd.openxmlformats-package.core-properties+xml": "coreprops",
        "application/vnd.openxmlformats-officedocument.custom-properties+xml": "custprops",
        "application/vnd.openxmlformats-officedocument.extended-properties+xml": "extprops",
        "application/vnd.openxmlformats-officedocument.customXmlProperties+xml": "TODO",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.customProperty": "TODO",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.comments+xml": "comments",
        "application/vnd.ms-excel.comments": "comments",
        "application/vnd.ms-excel.threadedcomments+xml": "threadedcomments",
        "application/vnd.ms-excel.person+xml": "people",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheetMetadata+xml": "metadata",
        "application/vnd.ms-excel.sheetMetadata": "metadata",
        "application/vnd.ms-excel.pivotTable": "TODO",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.pivotTable+xml": "TODO",
        "application/vnd.openxmlformats-officedocument.drawingml.chart+xml": "TODO",
        "application/vnd.ms-office.chartcolorstyle+xml": "TODO",
        "application/vnd.ms-office.chartstyle+xml": "TODO",
        "application/vnd.ms-office.chartex+xml": "TODO",
        "application/vnd.ms-excel.calcChain": "calcchains",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.calcChain+xml": "calcchains",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.printerSettings": "TODO",
        "application/vnd.ms-office.activeX": "TODO",
        "application/vnd.ms-office.activeX+xml": "TODO",
        "application/vnd.ms-excel.attachedToolbars": "TODO",
        "application/vnd.ms-excel.connections": "TODO",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.connections+xml": "TODO",
        "application/vnd.ms-excel.externalLink": "links",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.externalLink+xml": "links",
        "application/vnd.ms-excel.pivotCacheDefinition": "TODO",
        "application/vnd.ms-excel.pivotCacheRecords": "TODO",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.pivotCacheDefinition+xml": "TODO",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.pivotCacheRecords+xml": "TODO",
        "application/vnd.ms-excel.queryTable": "TODO",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.queryTable+xml": "TODO",
        "application/vnd.ms-excel.userNames": "TODO",
        "application/vnd.ms-excel.revisionHeaders": "TODO",
        "application/vnd.ms-excel.revisionLog": "TODO",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.revisionHeaders+xml": "TODO",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.revisionLog+xml": "TODO",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.userNames+xml": "TODO",
        "application/vnd.ms-excel.tableSingleCells": "TODO",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.tableSingleCells+xml": "TODO",
        "application/vnd.ms-excel.slicer": "TODO",
        "application/vnd.ms-excel.slicerCache": "TODO",
        "application/vnd.ms-excel.slicer+xml": "TODO",
        "application/vnd.ms-excel.slicerCache+xml": "TODO",
        "application/vnd.ms-excel.wsSortMap": "TODO",
        "application/vnd.ms-excel.table": "TODO",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.table+xml": "TODO",
        "application/vnd.openxmlformats-officedocument.theme+xml": "themes",
        "application/vnd.openxmlformats-officedocument.themeOverride+xml": "TODO",
        "application/vnd.ms-excel.Timeline+xml": "TODO",
        "application/vnd.ms-excel.TimelineCache+xml": "TODO",
        "application/vnd.ms-office.vbaProject": "vba",
        "application/vnd.ms-office.vbaProjectSignature": "TODO",
        "application/vnd.ms-office.volatileDependencies": "TODO",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.volatileDependencies+xml": "TODO",
        "application/vnd.ms-excel.controlproperties+xml": "TODO",
        "application/vnd.openxmlformats-officedocument.model+data": "TODO",
        "application/vnd.ms-excel.Survey+xml": "TODO",
        "application/vnd.openxmlformats-officedocument.drawing+xml": "drawings",
        "application/vnd.openxmlformats-officedocument.drawingml.chartshapes+xml": "TODO",
        "application/vnd.openxmlformats-officedocument.drawingml.diagramColors+xml": "TODO",
        "application/vnd.openxmlformats-officedocument.drawingml.diagramData+xml": "TODO",
        "application/vnd.openxmlformats-officedocument.drawingml.diagramLayout+xml": "TODO",
        "application/vnd.openxmlformats-officedocument.drawingml.diagramStyle+xml": "TODO",
        "application/vnd.openxmlformats-officedocument.vmlDrawing": "TODO",
        "application/vnd.openxmlformats-package.relationships+xml": "rels",
        "application/vnd.openxmlformats-officedocument.oleObject": "TODO",
        "image/png": "TODO",
        sheet: "js"
      }, wr = { workbooks: { xlsx: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml", xlsm: "application/vnd.ms-excel.sheet.macroEnabled.main+xml", xlsb: "application/vnd.ms-excel.sheet.binary.macroEnabled.main", xlam: "application/vnd.ms-excel.addin.macroEnabled.main+xml", xltx: "application/vnd.openxmlformats-officedocument.spreadsheetml.template.main+xml" }, strs: { xlsx: "application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml", xlsb: "application/vnd.ms-excel.sharedStrings" }, comments: { xlsx: "application/vnd.openxmlformats-officedocument.spreadsheetml.comments+xml", xlsb: "application/vnd.ms-excel.comments" }, sheets: { xlsx: "application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml", xlsb: "application/vnd.ms-excel.worksheet" }, charts: { xlsx: "application/vnd.openxmlformats-officedocument.spreadsheetml.chartsheet+xml", xlsb: "application/vnd.ms-excel.chartsheet" }, dialogs: { xlsx: "application/vnd.openxmlformats-officedocument.spreadsheetml.dialogsheet+xml", xlsb: "application/vnd.ms-excel.dialogsheet" }, macros: { xlsx: "application/vnd.ms-excel.macrosheet+xml", xlsb: "application/vnd.ms-excel.macrosheet" }, metadata: { xlsx: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheetMetadata+xml", xlsb: "application/vnd.ms-excel.sheetMetadata" }, styles: { xlsx: "application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml", xlsb: "application/vnd.ms-excel.styles" } };
      function ea() {
        return { workbooks: [], sheets: [], charts: [], dialogs: [], macros: [], rels: [], strs: [], comments: [], threadedcomments: [], links: [], coreprops: [], extprops: [], custprops: [], themes: [], styles: [], calcchains: [], vba: [], drawings: [], metadata: [], people: [], TODO: [], xmlns: "" };
      }
      function uo(e) {
        var r = ea();
        if (!e || !e.match) return r;
        var t = {};
        if ((e.match(Ne) || []).forEach(function(n) {
          var a = K(n);
          switch (a[0].replace(Ti, "<")) {
            case "<?xml":
              break;
            case "<Types":
              r.xmlns = a["xmlns" + (a[0].match(/<(\w+):/) || ["", ""])[1]];
              break;
            case "<Default":
              t[a.Extension] = a.ContentType;
              break;
            case "<Override":
              r[Qr[a.ContentType]] !== void 0 && r[Qr[a.ContentType]].push(a.PartName);
              break;
          }
        }), r.xmlns !== xe.CT) throw new Error("Unknown Namespace: " + r.xmlns);
        return r.calcchain = r.calcchains.length > 0 ? r.calcchains[0] : "", r.sst = r.strs.length > 0 ? r.strs[0] : "", r.style = r.styles.length > 0 ? r.styles[0] : "", r.defaults = t, delete r.calcchains, r;
      }
      function Sn(e, r) {
        var t = bi(Qr), n = [], a;
        n[n.length] = we, n[n.length] = X("Types", null, { xmlns: xe.CT, "xmlns:xsd": xe.xsd, "xmlns:xsi": xe.xsi }), n = n.concat([["xml", "application/xml"], ["bin", "application/vnd.ms-excel.sheet.binary.macroEnabled.main"], ["vml", "application/vnd.openxmlformats-officedocument.vmlDrawing"], ["data", "application/vnd.openxmlformats-officedocument.model+data"], ["bmp", "image/bmp"], ["png", "image/png"], ["gif", "image/gif"], ["emf", "image/x-emf"], ["wmf", "image/x-wmf"], ["jpg", "image/jpeg"], ["jpeg", "image/jpeg"], ["tif", "image/tiff"], ["tiff", "image/tiff"], ["pdf", "application/pdf"], ["rels", "application/vnd.openxmlformats-package.relationships+xml"]].map(function(c) {
          return X("Default", null, { Extension: c[0], ContentType: c[1] });
        }));
        var s = function(c) {
          e[c] && e[c].length > 0 && (a = e[c][0], n[n.length] = X("Override", null, { PartName: (a[0] == "/" ? "" : "/") + a, ContentType: wr[c][r.bookType] || wr[c].xlsx }));
        }, i = function(c) {
          (e[c] || []).forEach(function(f) {
            n[n.length] = X("Override", null, { PartName: (f[0] == "/" ? "" : "/") + f, ContentType: wr[c][r.bookType] || wr[c].xlsx });
          });
        }, o = function(c) {
          (e[c] || []).forEach(function(f) {
            n[n.length] = X("Override", null, { PartName: (f[0] == "/" ? "" : "/") + f, ContentType: t[c][0] });
          });
        };
        return s("workbooks"), i("sheets"), i("charts"), o("themes"), ["strs", "styles"].forEach(s), ["coreprops", "extprops", "custprops"].forEach(o), o("vba"), o("comments"), o("threadedcomments"), o("drawings"), i("metadata"), o("people"), n.length > 2 && (n[n.length] = "</Types>", n[1] = n[1].replace("/>", ">")), n.join("");
      }
      var Z = { WB: "http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument", SHEET: "http://sheetjs.openxmlformats.org/officeDocument/2006/relationships/officeDocument", HLINK: "http://schemas.openxmlformats.org/officeDocument/2006/relationships/hyperlink", VML: "http://schemas.openxmlformats.org/officeDocument/2006/relationships/vmlDrawing", XPATH: "http://schemas.openxmlformats.org/officeDocument/2006/relationships/externalLinkPath", XMISS: "http://schemas.microsoft.com/office/2006/relationships/xlExternalLinkPath/xlPathMissing", XLINK: "http://schemas.openxmlformats.org/officeDocument/2006/relationships/externalLink", CXML: "http://schemas.openxmlformats.org/officeDocument/2006/relationships/customXml", CXMLP: "http://schemas.openxmlformats.org/officeDocument/2006/relationships/customXmlProps", CMNT: "http://schemas.openxmlformats.org/officeDocument/2006/relationships/comments", CORE_PROPS: "http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties", EXT_PROPS: "http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties", CUST_PROPS: "http://schemas.openxmlformats.org/officeDocument/2006/relationships/custom-properties", SST: "http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings", STY: "http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles", THEME: "http://schemas.openxmlformats.org/officeDocument/2006/relationships/theme", CHART: "http://schemas.openxmlformats.org/officeDocument/2006/relationships/chart", CHARTEX: "http://schemas.microsoft.com/office/2014/relationships/chartEx", CS: "http://schemas.openxmlformats.org/officeDocument/2006/relationships/chartsheet", WS: ["http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet", "http://purl.oclc.org/ooxml/officeDocument/relationships/worksheet"], DS: "http://schemas.openxmlformats.org/officeDocument/2006/relationships/dialogsheet", MS: "http://schemas.microsoft.com/office/2006/relationships/xlMacrosheet", IMG: "http://schemas.openxmlformats.org/officeDocument/2006/relationships/image", DRAW: "http://schemas.openxmlformats.org/officeDocument/2006/relationships/drawing", XLMETA: "http://schemas.openxmlformats.org/officeDocument/2006/relationships/sheetMetadata", TCMNT: "http://schemas.microsoft.com/office/2017/10/relationships/threadedComment", PEOPLE: "http://schemas.microsoft.com/office/2017/10/relationships/person", VBA: "http://schemas.microsoft.com/office/2006/relationships/vbaProject" };
      function Zt(e) {
        var r = e.lastIndexOf("/");
        return e.slice(0, r + 1) + "_rels/" + e.slice(r + 1) + ".rels";
      }
      function Qt(e, r) {
        var t = { "!id": {} };
        if (!e) return t;
        r.charAt(0) !== "/" && (r = "/" + r);
        var n = {};
        return (e.match(Ne) || []).forEach(function(a) {
          var s = K(a);
          if (s[0] === "<Relationship") {
            var i = {};
            i.Type = s.Type, i.Target = s.Target, i.Id = s.Id, s.TargetMode && (i.TargetMode = s.TargetMode);
            var o = s.TargetMode === "External" ? s.Target : Gt(s.Target, r);
            t[o] = i, n[s.Id] = i;
          }
        }), t["!id"] = n, t;
      }
      function Mt(e) {
        var r = [we, X("Relationships", null, { xmlns: xe.RELS })];
        return De(e["!id"]).forEach(function(t) {
          r[r.length] = X("Relationship", null, e["!id"][t]);
        }), r.length > 2 && (r[r.length] = "</Relationships>", r[1] = r[1].replace("/>", ">")), r.join("");
      }
      function ne(e, r, t, n, a, s) {
        if (a || (a = {}), e["!id"] || (e["!id"] = {}), e["!idx"] || (e["!idx"] = 1), r < 0) for (r = e["!idx"]; e["!id"]["rId" + r]; ++r)
          ;
        if (e["!idx"] = r + 1, a.Id = "rId" + r, a.Type = n, a.Target = t, [Z.HLINK, Z.XPATH, Z.XMISS].indexOf(a.Type) > -1 && (a.TargetMode = "External"), e["!id"][a.Id]) throw new Error("Cannot rewrite rId " + r);
        return e["!id"][a.Id] = a, e[("/" + a.Target).replace("//", "/")] = a, r;
      }
      var ho = "application/vnd.oasis.opendocument.spreadsheet";
      function po(e, r) {
        for (var t = un(e), n, a; n = Yr.exec(t); ) switch (n[3]) {
          case "manifest":
            break;
          case "file-entry":
            if (a = K(n[0], !1), a.path == "/" && a.type !== ho) throw new Error("This OpenDocument is not a spreadsheet");
            break;
          case "encryption-data":
          case "algorithm":
          case "start-key-generation":
          case "key-derivation":
            throw new Error("Unsupported ODS Encryption");
          default:
            if (r && r.WTF) throw n;
        }
      }
      function mo(e) {
        var r = [we];
        r.push(`<manifest:manifest xmlns:manifest="urn:oasis:names:tc:opendocument:xmlns:manifest:1.0" manifest:version="1.2">
`), r.push(`  <manifest:file-entry manifest:full-path="/" manifest:version="1.2" manifest:media-type="application/vnd.oasis.opendocument.spreadsheet"/>
`);
        for (var t = 0; t < e.length; ++t) r.push('  <manifest:file-entry manifest:full-path="' + e[t][0] + '" manifest:media-type="' + e[t][1] + `"/>
`);
        return r.push("</manifest:manifest>"), r.join("");
      }
      function Cn(e, r, t) {
        return ['  <rdf:Description rdf:about="' + e + `">
`, '    <rdf:type rdf:resource="http://docs.oasis-open.org/ns/office/1.2/meta/' + (t || "odf") + "#" + r + `"/>
`, `  </rdf:Description>
`].join("");
      }
      function vo(e, r) {
        return ['  <rdf:Description rdf:about="' + e + `">
`, '    <ns0:hasPart xmlns:ns0="http://docs.oasis-open.org/ns/office/1.2/meta/pkg#" rdf:resource="' + r + `"/>
`, `  </rdf:Description>
`].join("");
      }
      function go(e) {
        var r = [we];
        r.push(`<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
`);
        for (var t = 0; t != e.length; ++t)
          r.push(Cn(e[t][0], e[t][1])), r.push(vo("", e[t][0]));
        return r.push(Cn("", "Document", "pkg")), r.push("</rdf:RDF>"), r.join("");
      }
      function _n() {
        return '<office:document-meta xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0" xmlns:meta="urn:oasis:names:tc:opendocument:xmlns:meta:1.0" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:xlink="http://www.w3.org/1999/xlink" office:version="1.2"><office:meta><meta:generator>SheetJS ' + le.version + "</meta:generator></office:meta></office:document-meta>";
      }
      var wt = [["cp:category", "Category"], ["cp:contentStatus", "ContentStatus"], ["cp:keywords", "Keywords"], ["cp:lastModifiedBy", "LastAuthor"], ["cp:lastPrinted", "LastPrinted"], ["cp:revision", "RevNumber"], ["cp:version", "Version"], ["dc:creator", "Author"], ["dc:description", "Comments"], ["dc:identifier", "Identifier"], ["dc:language", "Language"], ["dc:subject", "Subject"], ["dc:title", "Title"], ["dcterms:created", "CreatedDate", "date"], ["dcterms:modified", "ModifiedDate", "date"]], bo = (function() {
        for (var e = new Array(wt.length), r = 0; r < wt.length; ++r) {
          var t = wt[r], n = "(?:" + t[0].slice(0, t[0].indexOf(":")) + ":)" + t[0].slice(t[0].indexOf(":") + 1);
          e[r] = new RegExp("<" + n + "[^>]*>([\\s\\S]*?)</" + n + ">");
        }
        return e;
      })();
      function An(e) {
        var r = {};
        e = ie(e);
        for (var t = 0; t < wt.length; ++t) {
          var n = wt[t], a = e.match(bo[t]);
          a != null && a.length > 0 && (r[n[1]] = fe(a[1])), n[2] === "date" && r[n[1]] && (r[n[1]] = Se(r[n[1]]));
        }
        return r;
      }
      function ta(e, r, t, n, a) {
        a[e] != null || r == null || r === "" || (a[e] = r, r = he(r), n[n.length] = t ? X(e, r, t) : Ve(e, r));
      }
      function Fn(e, r) {
        var t = r || {}, n = [we, X("cp:coreProperties", null, { "xmlns:cp": xe.CORE_PROPS, "xmlns:dc": xe.dc, "xmlns:dcterms": xe.dcterms, "xmlns:dcmitype": xe.dcmitype, "xmlns:xsi": xe.xsi })], a = {};
        if (!e && !t.Props) return n.join("");
        e && (e.CreatedDate != null && ta("dcterms:created", typeof e.CreatedDate == "string" ? e.CreatedDate : Gr(e.CreatedDate, t.WTF), { "xsi:type": "dcterms:W3CDTF" }, n, a), e.ModifiedDate != null && ta("dcterms:modified", typeof e.ModifiedDate == "string" ? e.ModifiedDate : Gr(e.ModifiedDate, t.WTF), { "xsi:type": "dcterms:W3CDTF" }, n, a));
        for (var s = 0; s != wt.length; ++s) {
          var i = wt[s], o = t.Props && t.Props[i[1]] != null ? t.Props[i[1]] : e ? e[i[1]] : null;
          o === !0 ? o = "1" : o === !1 ? o = "0" : typeof o == "number" && (o = String(o)), o != null && ta(i[0], o, null, n, a);
        }
        return n.length > 2 && (n[n.length] = "</cp:coreProperties>", n[1] = n[1].replace("/>", ">")), n.join("");
      }
      var Tn = [["Application", "Application", "string"], ["AppVersion", "AppVersion", "string"], ["Company", "Company", "string"], ["DocSecurity", "DocSecurity", "string"], ["Manager", "Manager", "string"], ["HyperlinksChanged", "HyperlinksChanged", "bool"], ["SharedDoc", "SharedDoc", "bool"], ["LinksUpToDate", "LinksUpToDate", "bool"], ["ScaleCrop", "ScaleCrop", "bool"], ["HeadingPairs", "HeadingPairs", "raw"], ["TitlesOfParts", "TitlesOfParts", "raw"]];
      function wo(e, r, t, n) {
        var a = [];
        if (typeof e == "string") a = cn(e, n);
        else for (var s = 0; s < e.length; ++s) a = a.concat(e[s].map(function(l) {
          return { v: l };
        }));
        var i = typeof r == "string" ? cn(r, n).map(function(l) {
          return l.v;
        }) : r, o = 0, c = 0;
        if (i.length > 0) for (var f = 0; f !== a.length; f += 2) {
          switch (c = +a[f + 1].v, a[f].v) {
            case "Worksheets":
            case "工作表":
            case "Листы":
            case "أوراق العمل":
            case "ワークシート":
            case "גליונות עבודה":
            case "Arbeitsblätter":
            case "Çalışma Sayfaları":
            case "Feuilles de calcul":
            case "Fogli di lavoro":
            case "Folhas de cálculo":
            case "Planilhas":
            case "Regneark":
            case "Hojas de cálculo":
            case "Werkbladen":
              t.Worksheets = c, t.SheetNames = i.slice(o, o + c);
              break;
            case "Named Ranges":
            case "Rangos con nombre":
            case "名前付き一覧":
            case "Benannte Bereiche":
            case "Navngivne områder":
              t.NamedRanges = c, t.DefinedNames = i.slice(o, o + c);
              break;
            case "Charts":
            case "Diagramme":
              t.Chartsheets = c, t.ChartNames = i.slice(o, o + c);
              break;
          }
          o += c;
        }
      }
      function yo(e, r, t) {
        var n = {};
        return r || (r = {}), e = ie(e), Tn.forEach(function(a) {
          var s = (e.match(Yt(a[0])) || [])[1];
          switch (a[2]) {
            case "string":
              s && (r[a[1]] = fe(s));
              break;
            case "bool":
              r[a[1]] = s === "true";
              break;
            case "raw":
              var i = e.match(new RegExp("<" + a[0] + "[^>]*>([\\s\\S]*?)</" + a[0] + ">"));
              i && i.length > 0 && (n[a[1]] = i[1]);
              break;
          }
        }), n.HeadingPairs && n.TitlesOfParts && wo(n.HeadingPairs, n.TitlesOfParts, r, t), r;
      }
      function En(e) {
        var r = [], t = X;
        return e || (e = {}), e.Application = "SheetJS", r[r.length] = we, r[r.length] = X("Properties", null, { xmlns: xe.EXT_PROPS, "xmlns:vt": xe.vt }), Tn.forEach(function(n) {
          if (e[n[1]] !== void 0) {
            var a;
            switch (n[2]) {
              case "string":
                a = he(String(e[n[1]]));
                break;
              case "bool":
                a = e[n[1]] ? "true" : "false";
                break;
            }
            a !== void 0 && (r[r.length] = t(n[0], a));
          }
        }), r[r.length] = t("HeadingPairs", t("vt:vector", t("vt:variant", "<vt:lpstr>Worksheets</vt:lpstr>") + t("vt:variant", t("vt:i4", String(e.Worksheets))), { size: 2, baseType: "variant" })), r[r.length] = t("TitlesOfParts", t("vt:vector", e.SheetNames.map(function(n) {
          return "<vt:lpstr>" + he(n) + "</vt:lpstr>";
        }).join(""), { size: e.Worksheets, baseType: "lpstr" })), r.length > 2 && (r[r.length] = "</Properties>", r[1] = r[1].replace("/>", ">")), r.join("");
      }
      var ko = /<[^>]+>[^<]*/g;
      function xo(e, r) {
        var t = {}, n = "", a = e.match(ko);
        if (a) for (var s = 0; s != a.length; ++s) {
          var i = a[s], o = K(i);
          switch (o[0]) {
            case "<?xml":
              break;
            case "<Properties":
              break;
            case "<property":
              n = fe(o.name);
              break;
            case "</property>":
              n = null;
              break;
            default:
              if (i.indexOf("<vt:") === 0) {
                var c = i.split(">"), f = c[0].slice(4), l = c[1];
                switch (f) {
                  case "lpstr":
                  case "bstr":
                  case "lpwstr":
                    t[n] = fe(l);
                    break;
                  case "bool":
                    t[n] = de(l);
                    break;
                  case "i1":
                  case "i2":
                  case "i4":
                  case "i8":
                  case "int":
                  case "uint":
                    t[n] = parseInt(l, 10);
                    break;
                  case "r4":
                  case "r8":
                  case "decimal":
                    t[n] = parseFloat(l);
                    break;
                  case "filetime":
                  case "date":
                    t[n] = Se(l);
                    break;
                  case "cy":
                  case "error":
                    t[n] = fe(l);
                    break;
                  default:
                    if (f.slice(-1) == "/") break;
                    r.WTF && typeof console < "u" && console.warn("Unexpected", i, f, c);
                }
              } else if (i.slice(0, 2) !== "</") {
                if (r.WTF) throw new Error(i);
              }
          }
        }
        return t;
      }
      function Dn(e) {
        var r = [we, X("Properties", null, { xmlns: xe.CUST_PROPS, "xmlns:vt": xe.vt })];
        if (!e) return r.join("");
        var t = 1;
        return De(e).forEach(function(a) {
          ++t, r[r.length] = X("property", Ii(e[a]), { fmtid: "{D5CDD505-2E9C-101B-9397-08002B2CF9AE}", pid: t, name: he(a) });
        }), r.length > 2 && (r[r.length] = "</Properties>", r[1] = r[1].replace("/>", ">")), r.join("");
      }
      var So = [2, 3, 48, 49, 131, 139, 140, 245], ra = (function() {
        var e = { 1: 437, 2: 850, 3: 1252, 4: 1e4, 100: 852, 101: 866, 102: 865, 103: 861, 104: 895, 105: 620, 106: 737, 107: 857, 120: 950, 121: 949, 122: 936, 123: 932, 124: 874, 125: 1255, 126: 1256, 150: 10007, 151: 10029, 152: 10006, 200: 1250, 201: 1251, 202: 1254, 203: 1253, 0: 20127, 8: 865, 9: 437, 10: 850, 11: 437, 13: 437, 14: 850, 15: 437, 16: 850, 17: 437, 18: 850, 19: 932, 20: 850, 21: 437, 22: 850, 23: 865, 24: 437, 25: 437, 26: 850, 27: 437, 28: 863, 29: 850, 31: 852, 34: 852, 35: 852, 36: 860, 37: 850, 38: 866, 55: 850, 64: 852, 77: 936, 78: 949, 79: 950, 80: 874, 87: 1252, 88: 1252, 89: 1252, 108: 863, 134: 737, 135: 852, 136: 857, 204: 1257, 255: 16969 }, r = qa({ 1: 437, 2: 850, 3: 1252, 4: 1e4, 100: 852, 101: 866, 102: 865, 103: 861, 104: 895, 105: 620, 106: 737, 107: 857, 120: 950, 121: 949, 122: 936, 123: 932, 124: 874, 125: 1255, 126: 1256, 150: 10007, 151: 10029, 152: 10006, 200: 1250, 201: 1251, 202: 1254, 203: 1253, 0: 20127 });
        function t(o, c) {
          var f = [], l = $t(1);
          switch (c.type) {
            case "base64":
              l = ht(Qe(o));
              break;
            case "binary":
              l = ht(o);
              break;
            case "buffer":
            case "array":
              l = o;
              break;
          }
          $e(l, 0);
          var p = l._R(1), d = !!(p & 136), m = !1, w = !1;
          switch (p) {
            case 2:
              break;
            case 3:
              break;
            case 48:
              m = !0, d = !0;
              break;
            case 49:
              m = !0, d = !0;
              break;
            case 131:
              break;
            case 139:
              break;
            case 140:
              w = !0;
              break;
            case 245:
              break;
            default:
              throw new Error("DBF Unsupported Version: " + p.toString(16));
          }
          var u = 0, g = 521;
          p == 2 && (u = l._R(2)), l.l += 3, p != 2 && (u = l._R(4)), u > 1048576 && (u = 1e6), p != 2 && (g = l._R(2));
          var C = l._R(2), _ = c.codepage || 1252;
          p != 2 && (l.l += 16, l._R(1), l[l.l] !== 0 && (_ = e[l[l.l]]), l.l += 1, l.l += 2), w && (l.l += 36);
          for (var E = [], R = {}, N = Math.min(l.length, p == 2 ? 521 : g - 10 - (m ? 264 : 0)), F = w ? 32 : 11; l.l < N && l[l.l] != 13; )
            switch (R = {}, R.name = $cptable.utils.decode(_, l.slice(l.l, l.l + F)).replace(/[\u0000\r\n].*$/g, ""), l.l += F, R.type = String.fromCharCode(l._R(1)), p != 2 && !w && (R.offset = l._R(4)), R.len = l._R(1), p == 2 && (R.offset = l._R(2)), R.dec = l._R(1), R.name.length && E.push(R), p != 2 && (l.l += w ? 13 : 14), R.type) {
              case "B":
                (!m || R.len != 8) && c.WTF && console.log("Skipping " + R.name + ":" + R.type);
                break;
              case "G":
              case "P":
                c.WTF && console.log("Skipping " + R.name + ":" + R.type);
                break;
              case "+":
              case "0":
              case "@":
              case "C":
              case "D":
              case "F":
              case "I":
              case "L":
              case "M":
              case "N":
              case "O":
              case "T":
              case "Y":
                break;
              default:
                throw new Error("Unknown Field Type: " + R.type);
            }
          if (l[l.l] !== 13 && (l.l = g - 1), l._R(1) !== 13) throw new Error("DBF Terminator not found " + l.l + " " + l[l.l]);
          l.l = g;
          var S = 0, W = 0;
          for (f[0] = [], W = 0; W != E.length; ++W) f[0][W] = E[W].name;
          for (; u-- > 0; ) {
            if (l[l.l] === 42) {
              l.l += C;
              continue;
            }
            for (++l.l, f[++S] = [], W = 0, W = 0; W != E.length; ++W) {
              var M = l.slice(l.l, l.l + E[W].len);
              l.l += E[W].len, $e(M, 0);
              var L = $cptable.utils.decode(_, M);
              switch (E[W].type) {
                case "C":
                  L.trim().length && (f[S][W] = L.replace(/\s+$/, ""));
                  break;
                case "D":
                  L.length === 8 ? f[S][W] = new Date(+L.slice(0, 4), +L.slice(4, 6) - 1, +L.slice(6, 8)) : f[S][W] = L;
                  break;
                case "F":
                  f[S][W] = parseFloat(L.trim());
                  break;
                case "+":
                case "I":
                  f[S][W] = w ? M._R(-4, "i") ^ 2147483648 : M._R(4, "i");
                  break;
                case "L":
                  switch (L.trim().toUpperCase()) {
                    case "Y":
                    case "T":
                      f[S][W] = !0;
                      break;
                    case "N":
                    case "F":
                      f[S][W] = !1;
                      break;
                    case "":
                    case "?":
                      break;
                    default:
                      throw new Error("DBF Unrecognized L:|" + L + "|");
                  }
                  break;
                case "M":
                  if (!d) throw new Error("DBF Unexpected MEMO for type " + p.toString(16));
                  f[S][W] = "##MEMO##" + (w ? parseInt(L.trim(), 10) : M._R(4));
                  break;
                case "N":
                  L = L.replace(/\u0000/g, "").trim(), L && L != "." && (f[S][W] = +L || 0);
                  break;
                case "@":
                  f[S][W] = new Date(M._R(-8, "f") - 621356832e5);
                  break;
                case "T":
                  f[S][W] = new Date((M._R(4) - 2440588) * 864e5 + M._R(4));
                  break;
                case "Y":
                  f[S][W] = M._R(4, "i") / 1e4 + M._R(4, "i") / 1e4 * Math.pow(2, 32);
                  break;
                case "O":
                  f[S][W] = -M._R(-8, "f");
                  break;
                case "B":
                  if (m && E[W].len == 8) {
                    f[S][W] = M._R(8, "f");
                    break;
                  }
                case "G":
                case "P":
                  M.l += E[W].len;
                  break;
                case "0":
                  if (E[W].name === "_NullFlags") break;
                default:
                  throw new Error("DBF Unsupported data type " + E[W].type);
              }
            }
          }
          if (p != 2 && l.l < l.length && l[l.l++] != 26) throw new Error("DBF EOF Marker missing " + (l.l - 1) + " of " + l.length + " " + l[l.l - 1].toString(16));
          return c && c.sheetRows && (f = f.slice(0, c.sheetRows)), c.DBF = E, f;
        }
        function n(o, c) {
          var f = c || {};
          f.dateNF || (f.dateNF = "yyyymmdd");
          var l = Ot(t(o, f), f);
          return l["!cols"] = f.DBF.map(function(p) {
            return { wch: p.len, DBF: p };
          }), delete f.DBF, l;
        }
        function a(o, c) {
          try {
            return Dt(n(o, c), c);
          } catch (f) {
            if (c && c.WTF) throw f;
          }
          return { SheetNames: [], Sheets: {} };
        }
        var s = { B: 8, C: 250, L: 1, D: 8, "?": 0, "": 0 };
        function i(o, c) {
          var f = c || {};
          if (+f.codepage >= 0 && Rr(+f.codepage), f.type == "string") throw new Error("Cannot write DBF to JS string");
          var l = no(), p = wa(o, { header: 1, raw: !0, cellDates: !0 }), d = p[0], m = p.slice(1), w = o["!cols"] || [], u = 0, g = 0, C = 0, _ = 1;
          for (u = 0; u < d.length; ++u) {
            if (((w[u] || {}).DBF || {}).name) {
              d[u] = w[u].DBF.name, ++C;
              continue;
            }
            if (d[u] != null) {
              if (++C, typeof d[u] == "number" && (d[u] = d[u].toString(10)), typeof d[u] != "string") throw new Error("DBF Invalid column name " + d[u] + " |" + typeof d[u] + "|");
              if (d.indexOf(d[u]) !== u) {
                for (g = 0; g < 1024; ++g) if (d.indexOf(d[u] + "_" + g) == -1) {
                  d[u] += "_" + g;
                  break;
                }
              }
            }
          }
          var E = ke(o["!ref"]), R = [], N = [], F = [];
          for (u = 0; u <= E.e.c - E.s.c; ++u) {
            var S = "", W = "", M = 0, L = [];
            for (g = 0; g < m.length; ++g)
              m[g][u] != null && L.push(m[g][u]);
            if (L.length == 0 || d[u] == null) {
              R[u] = "?";
              continue;
            }
            for (g = 0; g < L.length; ++g) {
              switch (typeof L[g]) {
                case "number":
                  W = "B";
                  break;
                case "string":
                  W = "C";
                  break;
                case "boolean":
                  W = "L";
                  break;
                case "object":
                  W = L[g] instanceof Date ? "D" : "C";
                  break;
                default:
                  W = "C";
              }
              M = Math.max(M, String(L[g]).length), S = S && S != W ? "C" : W;
            }
            M > 250 && (M = 250), W = ((w[u] || {}).DBF || {}).type, W == "C" && w[u].DBF.len > M && (M = w[u].DBF.len), S == "B" && W == "N" && (S = "N", F[u] = w[u].DBF.dec, M = w[u].DBF.len), N[u] = S == "C" || W == "N" ? M : s[S] || 0, _ += N[u], R[u] = S;
          }
          var I = l.next(32);
          for (I._W(4, 318902576), I._W(4, m.length), I._W(2, 296 + 32 * C), I._W(2, _), u = 0; u < 4; ++u) I._W(4, 0);
          for (I._W(4, 0 | (+r[It] || 3) << 8), u = 0, g = 0; u < d.length; ++u)
            if (d[u] != null) {
              var z = l.next(32), G = (d[u].slice(-10) + "\0\0\0\0\0\0\0\0\0\0\0").slice(0, 11);
              z._W(1, G, "sbcs"), z._W(1, R[u] == "?" ? "C" : R[u], "sbcs"), z._W(4, g), z._W(1, N[u] || s[R[u]] || 0), z._W(1, F[u] || 0), z._W(1, 2), z._W(4, 0), z._W(1, 0), z._W(4, 0), z._W(4, 0), g += N[u] || s[R[u]] || 0;
            }
          var Q = l.next(264);
          for (Q._W(4, 13), u = 0; u < 65; ++u) Q._W(4, 0);
          for (u = 0; u < m.length; ++u) {
            var Y = l.next(_);
            for (Y._W(1, 0), g = 0; g < d.length; ++g)
              if (d[g] != null)
                switch (R[g]) {
                  case "L":
                    Y._W(1, m[u][g] == null ? 63 : m[u][g] ? 84 : 70);
                    break;
                  case "B":
                    Y._W(8, m[u][g] || 0, "f");
                    break;
                  case "N":
                    var ee = "0";
                    for (typeof m[u][g] == "number" && (ee = m[u][g].toFixed(F[g] || 0)), C = 0; C < N[g] - ee.length; ++C) Y._W(1, 32);
                    Y._W(1, ee, "sbcs");
                    break;
                  case "D":
                    m[u][g] ? (Y._W(4, ("0000" + m[u][g].getFullYear()).slice(-4), "sbcs"), Y._W(2, ("00" + (m[u][g].getMonth() + 1)).slice(-2), "sbcs"), Y._W(2, ("00" + m[u][g].getDate()).slice(-2), "sbcs")) : Y._W(8, "00000000", "sbcs");
                    break;
                  case "C":
                    var ue = String(m[u][g] != null ? m[u][g] : "").slice(0, N[g]);
                    for (Y._W(1, ue, "sbcs"), C = 0; C < N[g] - ue.length; ++C) Y._W(1, 32);
                    break;
                }
          }
          return l.next(1)._W(1, 26), l.end();
        }
        return { to_workbook: a, to_sheet: n, from_sheet: i };
      })(), On = (function() {
        var e = { AA: "À", BA: "Á", CA: "Â", DA: 195, HA: "Ä", JA: 197, AE: "È", BE: "É", CE: "Ê", HE: "Ë", AI: "Ì", BI: "Í", CI: "Î", HI: "Ï", AO: "Ò", BO: "Ó", CO: "Ô", DO: 213, HO: "Ö", AU: "Ù", BU: "Ú", CU: "Û", HU: "Ü", Aa: "à", Ba: "á", Ca: "â", Da: 227, Ha: "ä", Ja: 229, Ae: "è", Be: "é", Ce: "ê", He: "ë", Ai: "ì", Bi: "í", Ci: "î", Hi: "ï", Ao: "ò", Bo: "ó", Co: "ô", Do: 245, Ho: "ö", Au: "ù", Bu: "ú", Cu: "û", Hu: "ü", KC: "Ç", Kc: "ç", q: "æ", z: "œ", a: "Æ", j: "Œ", DN: 209, Dn: 241, Hy: 255, S: 169, c: 170, R: 174, "B ": 180, 0: 176, 1: 177, 2: 178, 3: 179, 5: 181, 6: 182, 7: 183, Q: 185, k: 186, b: 208, i: 216, l: 222, s: 240, y: 248, "!": 161, '"': 162, "#": 163, "(": 164, "%": 165, "'": 167, "H ": 168, "+": 171, ";": 187, "<": 188, "=": 189, ">": 190, "?": 191, "{": 223 }, r = new RegExp("\x1BN(" + De(e).join("|").replace(/\|\|\|/, "|\\||").replace(/([?()+])/g, "\\$1") + "|\\|)", "gm"), t = function(d, m) {
          var w = e[m];
          return typeof w == "number" ? Ea(w) : w;
        }, n = function(d, m, w) {
          var u = m.charCodeAt(0) - 32 << 4 | w.charCodeAt(0) - 48;
          return u == 59 ? d : Ea(u);
        };
        e["|"] = 254;
        function a(d, m) {
          switch (m.type) {
            case "base64":
              return s(Qe(d), m);
            case "binary":
              return s(d, m);
            case "buffer":
              return s(jt(d), m);
            case "array":
              return s(_t(d), m);
          }
          throw new Error("Unrecognized type " + m.type);
        }
        function s(d, m) {
          var w = d.split(/[\n\r]+/), u = -1, g = -1, C = 0, _ = 0, E = [], R = [], N = null, F = {}, S = [], W = [], M = [], L = 0, I;
          for (+m.codepage >= 0 && Rr(+m.codepage); C !== w.length; ++C) {
            L = 0;
            var z = w[C].trim().replace(/\x1B([\x20-\x2F])([\x30-\x3F])/g, n).replace(r, t), G = z.replace(/;;/g, "\0").split(";").map(function(Ke) {
              return Ke.replace(/\u0000/g, ";");
            }), Q = G[0], Y;
            if (z.length > 0) switch (Q) {
              case "ID":
                break;
              case "E":
                break;
              case "B":
                break;
              case "O":
                break;
              case "W":
                break;
              case "P":
                G[1].charAt(0) == "P" && R.push(z.slice(3).replace(/;;/g, ";"));
                break;
              case "C":
                var ee = !1, ue = !1, st = !1, ae = !1, te = -1, oe = -1;
                for (_ = 1; _ < G.length; ++_) switch (G[_].charAt(0)) {
                  case "A":
                    break;
                  case "X":
                    g = parseInt(G[_].slice(1)) - 1, ue = !0;
                    break;
                  case "Y":
                    for (u = parseInt(G[_].slice(1)) - 1, ue || (g = 0), I = E.length; I <= u; ++I) E[I] = [];
                    break;
                  case "K":
                    Y = G[_].slice(1), Y.charAt(0) === '"' ? Y = Y.slice(1, Y.length - 1) : Y === "TRUE" ? Y = !0 : Y === "FALSE" ? Y = !1 : isNaN(tt(Y)) ? isNaN(Xt(Y).getDate()) || (Y = Se(Y)) : (Y = tt(Y), N !== null && dr(N) && (Y = Br(Y))), typeof $cptable < "u" && typeof Y == "string" && (m || {}).type != "string" && (m || {}).codepage && (Y = $cptable.utils.decode(m.codepage, Y)), ee = !0;
                    break;
                  case "E":
                    ae = !0;
                    var Pe = xl(G[_].slice(1), { r: u, c: g });
                    E[u][g] = [E[u][g], Pe];
                    break;
                  case "S":
                    st = !0, E[u][g] = [E[u][g], "S5S"];
                    break;
                  case "G":
                    break;
                  case "R":
                    te = parseInt(G[_].slice(1)) - 1;
                    break;
                  case "C":
                    oe = parseInt(G[_].slice(1)) - 1;
                    break;
                  default:
                    if (m && m.WTF) throw new Error("SYLK bad record " + z);
                }
                if (ee && (E[u][g] && E[u][g].length == 2 ? E[u][g][0] = Y : E[u][g] = Y, N = null), st) {
                  if (ae) throw new Error("SYLK shared formula cannot have own formula");
                  var je = te > -1 && E[te][oe];
                  if (!je || !je[1]) throw new Error("SYLK shared formula cannot find base");
                  E[u][g][1] = Un(je[1], { r: u - te, c: g - oe });
                }
                break;
              case "F":
                var Ue = 0;
                for (_ = 1; _ < G.length; ++_) switch (G[_].charAt(0)) {
                  case "X":
                    g = parseInt(G[_].slice(1)) - 1, ++Ue;
                    break;
                  case "Y":
                    for (u = parseInt(G[_].slice(1)) - 1, I = E.length; I <= u; ++I) E[I] = [];
                    break;
                  case "M":
                    L = parseInt(G[_].slice(1)) / 20;
                    break;
                  case "F":
                    break;
                  case "G":
                    break;
                  case "P":
                    N = R[parseInt(G[_].slice(1))];
                    break;
                  case "S":
                    break;
                  case "D":
                    break;
                  case "N":
                    break;
                  case "W":
                    for (M = G[_].slice(1).split(" "), I = parseInt(M[0], 10); I <= parseInt(M[1], 10); ++I)
                      L = parseInt(M[2], 10), W[I - 1] = L === 0 ? { hidden: !0 } : { wch: L }, ia(W[I - 1]);
                    break;
                  case "C":
                    g = parseInt(G[_].slice(1)) - 1, W[g] || (W[g] = {});
                    break;
                  case "R":
                    u = parseInt(G[_].slice(1)) - 1, S[u] || (S[u] = {}), L > 0 ? (S[u].hpt = L, S[u].hpx = la(L)) : L === 0 && (S[u].hidden = !0);
                    break;
                  default:
                    if (m && m.WTF) throw new Error("SYLK bad record " + z);
                }
                Ue < 1 && (N = null);
                break;
              default:
                if (m && m.WTF) throw new Error("SYLK bad record " + z);
            }
          }
          return S.length > 0 && (F["!rows"] = S), W.length > 0 && (F["!cols"] = W), m && m.sheetRows && (E = E.slice(0, m.sheetRows)), [E, F];
        }
        function i(d, m) {
          var w = a(d, m), u = w[0], g = w[1], C = Ot(u, m);
          return De(g).forEach(function(_) {
            C[_] = g[_];
          }), C;
        }
        function o(d, m) {
          return Dt(i(d, m), m);
        }
        function c(d, m, w, u) {
          var g = "C;Y" + (w + 1) + ";X" + (u + 1) + ";K";
          switch (d.t) {
            case "n":
              g += d.v || 0, d.f && !d.F && (g += ";E" + Sl(d.f, { r: w, c: u }));
              break;
            case "b":
              g += d.v ? "TRUE" : "FALSE";
              break;
            case "e":
              g += d.w || d.v;
              break;
            case "d":
              g += '"' + (d.w || d.v) + '"';
              break;
            case "s":
              g += '"' + d.v.replace(/"/g, "").replace(/;/g, ";;") + '"';
              break;
          }
          return g;
        }
        function f(d, m) {
          m.forEach(function(w, u) {
            var g = "F;W" + (u + 1) + " " + (u + 1) + " ";
            w.hidden ? g += "0" : (typeof w.width == "number" && !w.wpx && (w.wpx = yr(w.width)), typeof w.wpx == "number" && !w.wch && (w.wch = tr(w.wpx)), typeof w.wch == "number" && (g += Math.round(w.wch))), g.charAt(g.length - 1) != " " && d.push(g);
          });
        }
        function l(d, m) {
          m.forEach(function(w, u) {
            var g = "F;";
            w.hidden ? g += "M0;" : w.hpt ? g += "M" + 20 * w.hpt + ";" : w.hpx && (g += "M" + 20 * oa(w.hpx) + ";"), g.length > 2 && d.push(g + "R" + (u + 1));
          });
        }
        function p(d, m) {
          var w = ["ID;PWXL;N;E"], u = [], g = ke(d["!ref"]), C, _ = Array.isArray(d), E = `\r
`;
          w.push("P;PGeneral"), w.push("F;P0;DG0G8;M255"), d["!cols"] && f(w, d["!cols"]), d["!rows"] && l(w, d["!rows"]), w.push("B;Y" + (g.e.r - g.s.r + 1) + ";X" + (g.e.c - g.s.c + 1) + ";D" + [g.s.c, g.s.r, g.e.c, g.e.r].join(" "));
          for (var R = g.s.r; R <= g.e.r; ++R)
            for (var N = g.s.c; N <= g.e.c; ++N) {
              var F = ge({ r: R, c: N });
              C = _ ? (d[R] || [])[N] : d[F], !(!C || C.v == null && (!C.f || C.F)) && u.push(c(C, d, R, N));
            }
          return w.join(E) + E + u.join(E) + E + "E" + E;
        }
        return { to_workbook: o, to_sheet: i, from_sheet: p };
      })(), Mn = /* @__PURE__ */ (function() {
        function e(s, i) {
          switch (i.type) {
            case "base64":
              return r(Qe(s), i);
            case "binary":
              return r(s, i);
            case "buffer":
              return r(jt(s), i);
            case "array":
              return r(_t(s), i);
          }
          throw new Error("Unrecognized type " + i.type);
        }
        function r(s, i) {
          for (var o = s.split(`
`), c = -1, f = -1, l = 0, p = []; l !== o.length; ++l) {
            if (o[l].trim() === "BOT") {
              p[++c] = [], f = 0;
              continue;
            }
            if (!(c < 0)) {
              var d = o[l].trim().split(","), m = d[0], w = d[1];
              ++l;
              for (var u = o[l] || ""; (u.match(/["]/g) || []).length & 1 && l < o.length - 1; ) u += `
` + o[++l];
              switch (u = u.trim(), +m) {
                case -1:
                  if (u === "BOT") {
                    p[++c] = [], f = 0;
                    continue;
                  } else if (u !== "EOD") throw new Error("Unrecognized DIF special command " + u);
                  break;
                case 0:
                  u === "TRUE" ? p[c][f] = !0 : u === "FALSE" ? p[c][f] = !1 : isNaN(tt(w)) ? isNaN(Xt(w).getDate()) ? p[c][f] = w : p[c][f] = Se(w) : p[c][f] = tt(w), ++f;
                  break;
                case 1:
                  u = u.slice(1, u.length - 1), u = u.replace(/""/g, '"'), u && u.match(/^=".*"$/) && (u = u.slice(2, -1)), p[c][f++] = u !== "" ? u : null;
                  break;
              }
              if (u === "EOD") break;
            }
          }
          return i && i.sheetRows && (p = p.slice(0, i.sheetRows)), p;
        }
        function t(s, i) {
          return Ot(e(s, i), i);
        }
        function n(s, i) {
          return Dt(t(s, i), i);
        }
        var a = /* @__PURE__ */ (function() {
          var s = function(c, f, l, p, d) {
            c.push(f), c.push(l + "," + p), c.push('"' + d.replace(/"/g, '""') + '"');
          }, i = function(c, f, l, p) {
            c.push(f + "," + l), c.push(f == 1 ? '"' + p.replace(/"/g, '""') + '"' : p);
          };
          return function(c) {
            var f = [], l = ke(c["!ref"]), p, d = Array.isArray(c);
            s(f, "TABLE", 0, 1, "sheetjs"), s(f, "VECTORS", 0, l.e.r - l.s.r + 1, ""), s(f, "TUPLES", 0, l.e.c - l.s.c + 1, ""), s(f, "DATA", 0, 0, "");
            for (var m = l.s.r; m <= l.e.r; ++m) {
              i(f, -1, 0, "BOT");
              for (var w = l.s.c; w <= l.e.c; ++w) {
                var u = ge({ r: m, c: w });
                if (p = d ? (c[m] || [])[w] : c[u], !p) {
                  i(f, 1, 0, "");
                  continue;
                }
                switch (p.t) {
                  case "n":
                    var g = p.w;
                    !g && p.v != null && (g = p.v), g == null ? p.f && !p.F ? i(f, 1, 0, "=" + p.f) : i(f, 1, 0, "") : i(f, 0, g, "V");
                    break;
                  case "b":
                    i(f, 0, p.v ? 1 : 0, p.v ? "TRUE" : "FALSE");
                    break;
                  case "s":
                    i(f, 1, 0, isNaN(p.v) ? p.v : '="' + p.v + '"');
                    break;
                  case "d":
                    p.w || (p.w = He(p.z || ce[14], Oe(Se(p.v)))), i(f, 0, p.w, "V");
                    break;
                  default:
                    i(f, 1, 0, "");
                }
              }
            }
            i(f, -1, 0, "EOD");
            var C = `\r
`, _ = f.join(C);
            return _;
          };
        })();
        return { to_workbook: n, to_sheet: t, from_sheet: a };
      })(), Rn = (function() {
        function e(p) {
          return p.replace(/\\b/g, "\\").replace(/\\c/g, ":").replace(/\\n/g, `
`);
        }
        function r(p) {
          return p.replace(/\\/g, "\\b").replace(/:/g, "\\c").replace(/\n/g, "\\n");
        }
        function t(p, d) {
          for (var m = p.split(`
`), w = -1, u = -1, g = 0, C = []; g !== m.length; ++g) {
            var _ = m[g].trim().split(":");
            if (_[0] === "cell") {
              var E = Fe(_[1]);
              if (C.length <= E.r) for (w = C.length; w <= E.r; ++w) C[w] || (C[w] = []);
              switch (w = E.r, u = E.c, _[2]) {
                case "t":
                  C[w][u] = e(_[3]);
                  break;
                case "v":
                  C[w][u] = +_[3];
                  break;
                case "vtf":
                  var R = _[_.length - 1];
                case "vtc":
                  switch (_[3]) {
                    case "nl":
                      C[w][u] = !!+_[4];
                      break;
                    default:
                      C[w][u] = +_[4];
                      break;
                  }
                  _[2] == "vtf" && (C[w][u] = [C[w][u], R]);
              }
            }
          }
          return d && d.sheetRows && (C = C.slice(0, d.sheetRows)), C;
        }
        function n(p, d) {
          return Ot(t(p, d), d);
        }
        function a(p, d) {
          return Dt(n(p, d), d);
        }
        var s = ["socialcalc:version:1.5", "MIME-Version: 1.0", "Content-Type: multipart/mixed; boundary=SocialCalcSpreadsheetControlSave"].join(`
`), i = ["--SocialCalcSpreadsheetControlSave", "Content-type: text/plain; charset=UTF-8"].join(`
`) + `
`, o = ["# SocialCalc Spreadsheet Control Save", "part:sheet"].join(`
`), c = "--SocialCalcSpreadsheetControlSave--";
        function f(p) {
          if (!p || !p["!ref"]) return "";
          for (var d = [], m = [], w, u = "", g = nt(p["!ref"]), C = Array.isArray(p), _ = g.s.r; _ <= g.e.r; ++_)
            for (var E = g.s.c; E <= g.e.c; ++E)
              if (u = ge({ r: _, c: E }), w = C ? (p[_] || [])[E] : p[u], !(!w || w.v == null || w.t === "z")) {
                switch (m = ["cell", u, "t"], w.t) {
                  case "s":
                  case "str":
                    m.push(r(w.v));
                    break;
                  case "n":
                    w.f ? (m[2] = "vtf", m[3] = "n", m[4] = w.v, m[5] = r(w.f)) : (m[2] = "v", m[3] = w.v);
                    break;
                  case "b":
                    m[2] = "vt" + (w.f ? "f" : "c"), m[3] = "nl", m[4] = w.v ? "1" : "0", m[5] = r(w.f || (w.v ? "TRUE" : "FALSE"));
                    break;
                  case "d":
                    var R = Oe(Se(w.v));
                    m[2] = "vtc", m[3] = "nd", m[4] = "" + R, m[5] = w.w || He(w.z || ce[14], R);
                    break;
                  case "e":
                    continue;
                }
                d.push(m.join(":"));
              }
          return d.push("sheet:c:" + (g.e.c - g.s.c + 1) + ":r:" + (g.e.r - g.s.r + 1) + ":tvf:1"), d.push("valueformat:1:text-wiki"), d.join(`
`);
        }
        function l(p) {
          return [s, i, o, i, f(p), c].join(`
`);
        }
        return { to_workbook: a, to_sheet: n, from_sheet: l };
      })(), er = /* @__PURE__ */ (function() {
        function e(l, p, d, m, w) {
          w.raw ? p[d][m] = l : l === "" || (l === "TRUE" ? p[d][m] = !0 : l === "FALSE" ? p[d][m] = !1 : isNaN(tt(l)) ? isNaN(Xt(l).getDate()) ? p[d][m] = l : p[d][m] = Se(l) : p[d][m] = tt(l));
        }
        function r(l, p) {
          var d = p || {}, m = [];
          if (!l || l.length === 0) return m;
          for (var w = l.split(/[\r\n]/), u = w.length - 1; u >= 0 && w[u].length === 0; ) --u;
          for (var g = 10, C = 0, _ = 0; _ <= u; ++_)
            C = w[_].indexOf(" "), C == -1 ? C = w[_].length : C++, g = Math.max(g, C);
          for (_ = 0; _ <= u; ++_) {
            m[_] = [];
            var E = 0;
            for (e(w[_].slice(0, g).trim(), m, _, E, d), E = 1; E <= (w[_].length - g) / 10 + 1; ++E) e(w[_].slice(g + (E - 1) * 10, g + E * 10).trim(), m, _, E, d);
          }
          return d.sheetRows && (m = m.slice(0, d.sheetRows)), m;
        }
        var t = { 44: ",", 9: "	", 59: ";", 124: "|" }, n = { 44: 3, 9: 2, 59: 1, 124: 0 };
        function a(l) {
          for (var p = {}, d = !1, m = 0, w = 0; m < l.length; ++m)
            (w = l.charCodeAt(m)) == 34 ? d = !d : !d && w in t && (p[w] = (p[w] || 0) + 1);
          w = [];
          for (m in p) Object.prototype.hasOwnProperty.call(p, m) && w.push([p[m], m]);
          if (!w.length) {
            p = n;
            for (m in p) Object.prototype.hasOwnProperty.call(p, m) && w.push([p[m], m]);
          }
          return w.sort(function(u, g) {
            return u[0] - g[0] || n[u[1]] - n[g[1]];
          }), t[w.pop()[1]] || 44;
        }
        function s(l, p) {
          var d = p || {}, m = "", w = d.dense ? [] : {}, u = { s: { c: 0, r: 0 }, e: { c: 0, r: 0 } };
          l.slice(0, 4) == "sep=" ? l.charCodeAt(5) == 13 && l.charCodeAt(6) == 10 ? (m = l.charAt(4), l = l.slice(7)) : l.charCodeAt(5) == 13 || l.charCodeAt(5) == 10 ? (m = l.charAt(4), l = l.slice(6)) : m = a(l.slice(0, 1024)) : d && d.FS ? m = d.FS : m = a(l.slice(0, 1024));
          var g = 0, C = 0, _ = 0, E = 0, R = 0, N = m.charCodeAt(0), F = !1, S = 0, W = l.charCodeAt(0);
          l = l.replace(/\r\n/gm, `
`);
          var M = d.dateNF != null ? pi(d.dateNF) : null;
          function L() {
            var I = l.slice(E, R), z = {};
            if (I.charAt(0) == '"' && I.charAt(I.length - 1) == '"' && (I = I.slice(1, -1).replace(/""/g, '"')), I.length === 0) z.t = "z";
            else if (d.raw)
              z.t = "s", z.v = I;
            else if (I.trim().length === 0)
              z.t = "s", z.v = I;
            else if (I.charCodeAt(0) == 61)
              I.charCodeAt(1) == 34 && I.charCodeAt(I.length - 1) == 34 ? (z.t = "s", z.v = I.slice(2, -1).replace(/""/g, '"')) : _l(I) ? (z.t = "n", z.f = I.slice(1)) : (z.t = "s", z.v = I);
            else if (I == "TRUE")
              z.t = "b", z.v = !0;
            else if (I == "FALSE")
              z.t = "b", z.v = !1;
            else if (!isNaN(_ = tt(I)))
              z.t = "n", d.cellText !== !1 && (z.w = I), z.v = _;
            else if (!isNaN(Xt(I).getDate()) || M && I.match(M)) {
              z.z = d.dateNF || ce[14];
              var G = 0;
              M && I.match(M) && (I = mi(I, d.dateNF, I.match(M) || []), G = 1), d.cellDates ? (z.t = "d", z.v = Se(I, G)) : (z.t = "n", z.v = Oe(Se(I, G))), d.cellText !== !1 && (z.w = He(z.z, z.v instanceof Date ? Oe(z.v) : z.v)), d.cellNF || delete z.z;
            } else
              z.t = "s", z.v = I;
            if (z.t == "z" || (d.dense ? (w[g] || (w[g] = []), w[g][C] = z) : w[ge({ c: C, r: g })] = z), E = R + 1, W = l.charCodeAt(E), u.e.c < C && (u.e.c = C), u.e.r < g && (u.e.r = g), S == N) ++C;
            else if (C = 0, ++g, d.sheetRows && d.sheetRows <= g) return !0;
          }
          e: for (; R < l.length; ++R) switch (S = l.charCodeAt(R)) {
            case 34:
              W === 34 && (F = !F);
              break;
            case N:
            case 10:
            case 13:
              if (!F && L()) break e;
              break;
          }
          return R - E > 0 && L(), w["!ref"] = ye(u), w;
        }
        function i(l, p) {
          return !(p && p.PRN) || p.FS || l.slice(0, 4) == "sep=" || l.indexOf("	") >= 0 || l.indexOf(",") >= 0 || l.indexOf(";") >= 0 ? s(l, p) : Ot(r(l, p), p);
        }
        function o(l, p) {
          var d = "", m = p.type == "string" ? [0, 0, 0, 0] : da(l, p);
          switch (p.type) {
            case "base64":
              d = Qe(l);
              break;
            case "binary":
              d = l;
              break;
            case "buffer":
              p.codepage == 65001 ? d = l.toString("utf8") : p.codepage && typeof $cptable < "u" ? d = $cptable.utils.decode(p.codepage, l) : d = jt(l);
              break;
            case "array":
              d = _t(l);
              break;
            case "string":
              d = l;
              break;
            default:
              throw new Error("Unrecognized type " + p.type);
          }
          return m[0] == 239 && m[1] == 187 && m[2] == 191 ? d = ie(d.slice(3)) : p.type != "string" && p.type != "buffer" && p.codepage == 65001 ? d = ie(d) : p.type == "binary" && typeof $cptable < "u" && p.codepage && (d = $cptable.utils.decode(p.codepage, $cptable.utils.encode(28591, d))), d.slice(0, 19) == "socialcalc:version:" ? Rn.to_sheet(p.type == "string" ? d : ie(d), p) : i(d, p);
        }
        function c(l, p) {
          return Dt(o(l, p), p);
        }
        function f(l) {
          for (var p = [], d = ke(l["!ref"]), m, w = Array.isArray(l), u = d.s.r; u <= d.e.r; ++u) {
            for (var g = [], C = d.s.c; C <= d.e.c; ++C) {
              var _ = ge({ r: u, c: C });
              if (m = w ? (l[u] || [])[C] : l[_], !m || m.v == null) {
                g.push("          ");
                continue;
              }
              for (var E = (m.w || (Et(m), m.w) || "").slice(0, 10); E.length < 10; ) E += " ";
              g.push(E + (C === 0 ? " " : ""));
            }
            p.push(g.join(""));
          }
          return p.join(`
`);
        }
        return { to_workbook: c, to_sheet: o, from_sheet: f };
      })();
      function Co(e, r) {
        var t = r || {}, n = !!t.WTF;
        t.WTF = !0;
        try {
          var a = On.to_workbook(e, t);
          return t.WTF = n, a;
        } catch (s) {
          if (t.WTF = n, !s.message.match(/SYLK bad record ID/) && n) throw s;
          return er.to_workbook(e, r);
        }
      }
      function _o(e) {
        var r = {}, t = e.match(Ne), n = 0, a = !1;
        if (t) for (; n != t.length; ++n) {
          var s = K(t[n]);
          switch (s[0].replace(/\w*:/g, "")) {
            case "<condense":
              break;
            case "<extend":
              break;
            case "<shadow":
              if (!s.val) break;
            case "<shadow>":
            case "<shadow/>":
              r.shadow = 1;
              break;
            case "</shadow>":
              break;
            case "<charset":
              if (s.val == "1") break;
              r.cp = Mr[parseInt(s.val, 10)];
              break;
            case "<outline":
              if (!s.val) break;
            case "<outline>":
            case "<outline/>":
              r.outline = 1;
              break;
            case "</outline>":
              break;
            case "<rFont":
              r.name = s.val;
              break;
            case "<sz":
              r.sz = s.val;
              break;
            case "<strike":
              if (!s.val) break;
            case "<strike>":
            case "<strike/>":
              r.strike = 1;
              break;
            case "</strike>":
              break;
            case "<u":
              if (!s.val) break;
              switch (s.val) {
                case "double":
                  r.uval = "double";
                  break;
                case "singleAccounting":
                  r.uval = "single-accounting";
                  break;
                case "doubleAccounting":
                  r.uval = "double-accounting";
                  break;
              }
            case "<u>":
            case "<u/>":
              r.u = 1;
              break;
            case "</u>":
              break;
            case "<b":
              if (s.val == "0") break;
            case "<b>":
            case "<b/>":
              r.b = 1;
              break;
            case "</b>":
              break;
            case "<i":
              if (s.val == "0") break;
            case "<i>":
            case "<i/>":
              r.i = 1;
              break;
            case "</i>":
              break;
            case "<color":
              s.rgb && (r.color = s.rgb.slice(2, 8));
              break;
            case "<color>":
            case "<color/>":
            case "</color>":
              break;
            case "<family":
              r.family = s.val;
              break;
            case "<family>":
            case "<family/>":
            case "</family>":
              break;
            case "<vertAlign":
              r.valign = s.val;
              break;
            case "<vertAlign>":
            case "<vertAlign/>":
            case "</vertAlign>":
              break;
            case "<scheme":
              break;
            case "<scheme>":
            case "<scheme/>":
            case "</scheme>":
              break;
            case "<extLst":
            case "<extLst>":
            case "</extLst>":
              break;
            case "<ext":
              a = !0;
              break;
            case "</ext>":
              a = !1;
              break;
            default:
              if (s[0].charCodeAt(1) !== 47 && !a) throw new Error("Unrecognized rich format " + s[0]);
          }
        }
        return r;
      }
      var Ao = (function() {
        var e = Yt("t"), r = Yt("rPr");
        function t(s) {
          var i = s.match(e);
          if (!i) return { t: "s", v: "" };
          var o = { t: "s", v: fe(i[1]) }, c = s.match(r);
          return c && (o.s = _o(c[1])), o;
        }
        var n = /<(?:\w+:)?r>/g, a = /<\/(?:\w+:)?r>/;
        return function(i) {
          return i.replace(n, "").split(a).map(t).filter(function(o) {
            return o.v;
          });
        };
      })(), Fo = /* @__PURE__ */ (function() {
        var r = /(\r\n|\n)/g;
        function t(a, s, i) {
          var o = [];
          a.u && o.push("text-decoration: underline;"), a.uval && o.push("text-underline-style:" + a.uval + ";"), a.sz && o.push("font-size:" + a.sz + "pt;"), a.outline && o.push("text-effect: outline;"), a.shadow && o.push("text-shadow: auto;"), s.push('<span style="' + o.join("") + '">'), a.b && (s.push("<b>"), i.push("</b>")), a.i && (s.push("<i>"), i.push("</i>")), a.strike && (s.push("<s>"), i.push("</s>"));
          var c = a.valign || "";
          return c == "superscript" || c == "super" ? c = "sup" : c == "subscript" && (c = "sub"), c != "" && (s.push("<" + c + ">"), i.push("</" + c + ">")), i.push("</span>"), a;
        }
        function n(a) {
          var s = [[], a.v, []];
          return a.v ? (a.s && t(a.s, s[0], s[2]), s[0].join("") + s[1].replace(r, "<br/>") + s[2].join("")) : "";
        }
        return function(s) {
          return s.map(n).join("");
        };
      })(), To = /<(?:\w+:)?t[^>]*>([^<]*)<\/(?:\w+:)?t>/g, Eo = /<(?:\w+:)?r>/, Do = /<(?:\w+:)?rPh.*?>([\s\S]*?)<\/(?:\w+:)?rPh>/g;
      function aa(e, r) {
        var t = r ? r.cellHTML : !0, n = {};
        return e ? (e.match(/^\s*<(?:\w+:)?t[^>]*>/) ? (n.t = fe(ie(e.slice(e.indexOf(">") + 1).split(/<\/(?:\w+:)?t>/)[0] || "")), n.r = ie(e), t && (n.h = Xr(n.t))) : e.match(Eo) && (n.r = ie(e), n.t = fe(ie((e.replace(Do, "").match(To) || []).join("").replace(Ne, ""))), t && (n.h = Fo(Ao(n.r)))), n) : { t: "" };
      }
      var Oo = /<(?:\w+:)?sst([^>]*)>([\s\S]*)<\/(?:\w+:)?sst>/, Mo = /<(?:\w+:)?(?:si|sstItem)>/g, Ro = /<\/(?:\w+:)?(?:si|sstItem)>/;
      function No(e, r) {
        var t = [], n = "";
        if (!e) return t;
        var a = e.match(Oo);
        if (a) {
          n = a[2].replace(Mo, "").split(Ro);
          for (var s = 0; s != n.length; ++s) {
            var i = aa(n[s].trim(), r);
            i != null && (t[t.length] = i);
          }
          a = K(a[1]), t.Count = a.count, t.Unique = a.uniqueCount;
        }
        return t;
      }
      var Po = /^\s|\s$|[\t\n\r]/;
      function Nn(e, r) {
        if (!r.bookSST) return "";
        var t = [we];
        t[t.length] = X("sst", null, { xmlns: bt[0], count: e.Count, uniqueCount: e.Unique });
        for (var n = 0; n != e.length; ++n)
          if (e[n] != null) {
            var a = e[n], s = "<si>";
            a.r ? s += a.r : (s += "<t", a.t || (a.t = ""), a.t.match(Po) && (s += ' xml:space="preserve"'), s += ">" + he(a.t) + "</t>"), s += "</si>", t[t.length] = s;
          }
        return t.length > 2 && (t[t.length] = "</sst>", t[1] = t[1].replace("/>", ">")), t.join("");
      }
      function Wo(e) {
        var r = e.slice(e[0] === "#" ? 1 : 0).slice(0, 6);
        return [parseInt(r.slice(0, 2), 16), parseInt(r.slice(2, 4), 16), parseInt(r.slice(4, 6), 16)];
      }
      function Io(e) {
        for (var r = 0, t = 1; r != 3; ++r) t = t * 256 + (e[r] > 255 ? 255 : e[r] < 0 ? 0 : e[r]);
        return t.toString(16).toUpperCase().slice(1);
      }
      function Lo(e) {
        var r = e[0] / 255, t = e[1] / 255, n = e[2] / 255, a = Math.max(r, t, n), s = Math.min(r, t, n), i = a - s;
        if (i === 0) return [0, 0, r];
        var o = 0, c = 0, f = a + s;
        switch (c = i / (f > 1 ? 2 - f : f), a) {
          case r:
            o = ((t - n) / i + 6) % 6;
            break;
          case t:
            o = (n - r) / i + 2;
            break;
          case n:
            o = (r - t) / i + 4;
            break;
        }
        return [o / 6, c, f / 2];
      }
      function $o(e) {
        var r = e[0], t = e[1], n = e[2], a = t * 2 * (n < 0.5 ? n : 1 - n), s = n - a / 2, i = [s, s, s], o = 6 * r, c;
        if (t !== 0) switch (o | 0) {
          case 0:
          case 6:
            c = a * o, i[0] += a, i[1] += c;
            break;
          case 1:
            c = a * (2 - o), i[0] += c, i[1] += a;
            break;
          case 2:
            c = a * (o - 2), i[1] += a, i[2] += c;
            break;
          case 3:
            c = a * (4 - o), i[1] += c, i[2] += a;
            break;
          case 4:
            c = a * (o - 4), i[2] += a, i[0] += c;
            break;
          case 5:
            c = a * (6 - o), i[2] += c, i[0] += a;
            break;
        }
        for (var f = 0; f != 3; ++f) i[f] = Math.round(i[f] * 255);
        return i;
      }
      function na(e, r) {
        if (r === 0) return e;
        var t = Lo(Wo(e));
        return r < 0 ? t[2] = t[2] * (1 + r) : t[2] = 1 - (1 - t[2]) * (1 - r), Io($o(t));
      }
      var zo = 6, jo = 15, Uo = 1, Me = zo;
      function yr(e) {
        return Math.floor((e + Math.round(128 / Me) / 256) * Me);
      }
      function tr(e) {
        return Math.floor((e - 5) / Me * 100 + 0.5) / 100;
      }
      function kr(e) {
        return Math.round((e * Me + 5) / Me * 256) / 256;
      }
      function sa(e) {
        return kr(tr(yr(e)));
      }
      function Bo(e) {
        var r = Math.abs(e - sa(e)), t = Me;
        if (r > 5e-3) for (Me = Uo; Me < jo; ++Me) Math.abs(e - sa(e)) <= r && (r = Math.abs(e - sa(e)), t = Me);
        Me = t;
      }
      function ia(e) {
        e.width ? (e.wpx = yr(e.width), e.wch = tr(e.wpx), e.MDW = Me) : e.wpx ? (e.wch = tr(e.wpx), e.width = kr(e.wch), e.MDW = Me) : typeof e.wch == "number" && (e.width = kr(e.wch), e.wpx = yr(e.width), e.MDW = Me), e.customWidth && delete e.customWidth;
      }
      var Ho = 96, Pn = Ho;
      function oa(e) {
        return e * 96 / Pn;
      }
      function la(e) {
        return e * Pn / 96;
      }
      function Vo(e, r, t, n) {
        r.Borders = [];
        var a = {}, s = !1;
        (e[0].match(Ne) || []).forEach(function(i) {
          var o = K(i);
          switch (rt(o[0])) {
            case "<borders":
            case "<borders>":
            case "</borders>":
              break;
            case "<border":
            case "<border>":
            case "<border/>":
              a = {}, o.diagonalUp && (a.diagonalUp = de(o.diagonalUp)), o.diagonalDown && (a.diagonalDown = de(o.diagonalDown)), r.Borders.push(a);
              break;
            case "</border>":
              break;
            case "<left/>":
              break;
            case "<left":
            case "<left>":
              break;
            case "</left>":
              break;
            case "<right/>":
              break;
            case "<right":
            case "<right>":
              break;
            case "</right>":
              break;
            case "<top/>":
              break;
            case "<top":
            case "<top>":
              break;
            case "</top>":
              break;
            case "<bottom/>":
              break;
            case "<bottom":
            case "<bottom>":
              break;
            case "</bottom>":
              break;
            case "<diagonal":
            case "<diagonal>":
            case "<diagonal/>":
              break;
            case "</diagonal>":
              break;
            case "<horizontal":
            case "<horizontal>":
            case "<horizontal/>":
              break;
            case "</horizontal>":
              break;
            case "<vertical":
            case "<vertical>":
            case "<vertical/>":
              break;
            case "</vertical>":
              break;
            case "<start":
            case "<start>":
            case "<start/>":
              break;
            case "</start>":
              break;
            case "<end":
            case "<end>":
            case "<end/>":
              break;
            case "</end>":
              break;
            case "<color":
            case "<color>":
              break;
            case "<color/>":
            case "</color>":
              break;
            case "<extLst":
            case "<extLst>":
            case "</extLst>":
              break;
            case "<ext":
              s = !0;
              break;
            case "</ext>":
              s = !1;
              break;
            default:
              if (n && n.WTF && !s)
                throw new Error("unrecognized " + o[0] + " in borders");
          }
        });
      }
      function Xo(e, r, t, n) {
        r.Fills = [];
        var a = {}, s = !1;
        (e[0].match(Ne) || []).forEach(function(i) {
          var o = K(i);
          switch (rt(o[0])) {
            case "<fills":
            case "<fills>":
            case "</fills>":
              break;
            case "<fill>":
            case "<fill":
            case "<fill/>":
              a = {}, r.Fills.push(a);
              break;
            case "</fill>":
              break;
            case "<gradientFill>":
              break;
            case "<gradientFill":
            case "</gradientFill>":
              r.Fills.push(a), a = {};
              break;
            case "<patternFill":
            case "<patternFill>":
              o.patternType && (a.patternType = o.patternType);
              break;
            case "<patternFill/>":
            case "</patternFill>":
              break;
            case "<bgColor":
              a.bgColor || (a.bgColor = {}), o.indexed && (a.bgColor.indexed = parseInt(o.indexed, 10)), o.theme && (a.bgColor.theme = parseInt(o.theme, 10)), o.tint && (a.bgColor.tint = parseFloat(o.tint)), o.rgb && (a.bgColor.rgb = o.rgb.slice(-6));
              break;
            case "<bgColor/>":
            case "</bgColor>":
              break;
            case "<fgColor":
              a.fgColor || (a.fgColor = {}), o.theme && (a.fgColor.theme = parseInt(o.theme, 10)), o.tint && (a.fgColor.tint = parseFloat(o.tint)), o.rgb != null && (a.fgColor.rgb = o.rgb.slice(-6));
              break;
            case "<fgColor/>":
            case "</fgColor>":
              break;
            case "<stop":
            case "<stop/>":
              break;
            case "</stop>":
              break;
            case "<color":
            case "<color/>":
              break;
            case "</color>":
              break;
            case "<extLst":
            case "<extLst>":
            case "</extLst>":
              break;
            case "<ext":
              s = !0;
              break;
            case "</ext>":
              s = !1;
              break;
            default:
              if (n && n.WTF && !s)
                throw new Error("unrecognized " + o[0] + " in fills");
          }
        });
      }
      function Go(e, r, t, n) {
        r.Fonts = [];
        var a = {}, s = !1;
        (e[0].match(Ne) || []).forEach(function(i) {
          var o = K(i);
          switch (rt(o[0])) {
            case "<fonts":
            case "<fonts>":
            case "</fonts>":
              break;
            case "<font":
            case "<font>":
              break;
            case "</font>":
            case "<font/>":
              r.Fonts.push(a), a = {};
              break;
            case "<name":
              o.val && (a.name = ie(o.val));
              break;
            case "<name/>":
            case "</name>":
              break;
            case "<b":
              a.bold = o.val ? de(o.val) : 1;
              break;
            case "<b/>":
              a.bold = 1;
              break;
            case "<i":
              a.italic = o.val ? de(o.val) : 1;
              break;
            case "<i/>":
              a.italic = 1;
              break;
            case "<u":
              switch (o.val) {
                case "none":
                  a.underline = 0;
                  break;
                case "single":
                  a.underline = 1;
                  break;
                case "double":
                  a.underline = 2;
                  break;
                case "singleAccounting":
                  a.underline = 33;
                  break;
                case "doubleAccounting":
                  a.underline = 34;
                  break;
              }
              break;
            case "<u/>":
              a.underline = 1;
              break;
            case "<strike":
              a.strike = o.val ? de(o.val) : 1;
              break;
            case "<strike/>":
              a.strike = 1;
              break;
            case "<outline":
              a.outline = o.val ? de(o.val) : 1;
              break;
            case "<outline/>":
              a.outline = 1;
              break;
            case "<shadow":
              a.shadow = o.val ? de(o.val) : 1;
              break;
            case "<shadow/>":
              a.shadow = 1;
              break;
            case "<condense":
              a.condense = o.val ? de(o.val) : 1;
              break;
            case "<condense/>":
              a.condense = 1;
              break;
            case "<extend":
              a.extend = o.val ? de(o.val) : 1;
              break;
            case "<extend/>":
              a.extend = 1;
              break;
            case "<sz":
              o.val && (a.sz = +o.val);
              break;
            case "<sz/>":
            case "</sz>":
              break;
            case "<vertAlign":
              o.val && (a.vertAlign = o.val);
              break;
            case "<vertAlign/>":
            case "</vertAlign>":
              break;
            case "<family":
              o.val && (a.family = parseInt(o.val, 10));
              break;
            case "<family/>":
            case "</family>":
              break;
            case "<scheme":
              o.val && (a.scheme = o.val);
              break;
            case "<scheme/>":
            case "</scheme>":
              break;
            case "<charset":
              if (o.val == "1") break;
              o.codepage = Mr[parseInt(o.val, 10)];
              break;
            case "<color":
              if (a.color || (a.color = {}), o.auto && (a.color.auto = de(o.auto)), o.rgb) a.color.rgb = o.rgb.slice(-6);
              else if (o.indexed) {
                a.color.index = parseInt(o.indexed, 10);
                var c = Kr[a.color.index];
                a.color.index == 81 && (c = Kr[1]), c || (c = Kr[1]), a.color.rgb = c[0].toString(16) + c[1].toString(16) + c[2].toString(16);
              } else o.theme && (a.color.theme = parseInt(o.theme, 10), o.tint && (a.color.tint = parseFloat(o.tint)), o.theme && t.themeElements && t.themeElements.clrScheme && (a.color.rgb = na(t.themeElements.clrScheme[a.color.theme].rgb, a.color.tint || 0)));
              break;
            case "<color/>":
            case "</color>":
              break;
            case "<AlternateContent":
              s = !0;
              break;
            case "</AlternateContent>":
              s = !1;
              break;
            case "<extLst":
            case "<extLst>":
            case "</extLst>":
              break;
            case "<ext":
              s = !0;
              break;
            case "</ext>":
              s = !1;
              break;
            default:
              if (n && n.WTF && !s)
                throw new Error("unrecognized " + o[0] + " in fonts");
          }
        });
      }
      function Yo(e, r, t) {
        r.NumberFmt = [];
        for (var n = De(ce), a = 0; a < n.length; ++a) r.NumberFmt[n[a]] = ce[n[a]];
        var s = e[0].match(Ne);
        if (s)
          for (a = 0; a < s.length; ++a) {
            var i = K(s[a]);
            switch (rt(i[0])) {
              case "<numFmts":
              case "</numFmts>":
              case "<numFmts/>":
              case "<numFmts>":
                break;
              case "<numFmt":
                {
                  var o = fe(ie(i.formatCode)), c = parseInt(i.numFmtId, 10);
                  if (r.NumberFmt[c] = o, c > 0) {
                    if (c > 392) {
                      for (c = 392; c > 60 && r.NumberFmt[c] != null; --c) ;
                      r.NumberFmt[c] = o;
                    }
                    Ht(o, c);
                  }
                }
                break;
              case "</numFmt>":
                break;
              default:
                if (t.WTF) throw new Error("unrecognized " + i[0] + " in numFmts");
            }
          }
      }
      function qo(e) {
        var r = ["<numFmts>"];
        return [[5, 8], [23, 26], [41, 44], [50, 392]].forEach(function(t) {
          for (var n = t[0]; n <= t[1]; ++n) e[n] != null && (r[r.length] = X("numFmt", null, { numFmtId: n, formatCode: he(e[n]) }));
        }), r.length === 1 ? "" : (r[r.length] = "</numFmts>", r[0] = X("numFmts", null, { count: r.length - 2 }).replace("/>", ">"), r.join(""));
      }
      var xr = ["numFmtId", "fillId", "fontId", "borderId", "xfId"], Sr = ["applyAlignment", "applyBorder", "applyFill", "applyFont", "applyNumberFormat", "applyProtection", "pivotButton", "quotePrefix"];
      function Jo(e, r, t) {
        r.CellXf = [];
        var n, a = !1;
        (e[0].match(Ne) || []).forEach(function(s) {
          var i = K(s), o = 0;
          switch (rt(i[0])) {
            case "<cellXfs":
            case "<cellXfs>":
            case "<cellXfs/>":
            case "</cellXfs>":
              break;
            case "<xf":
            case "<xf/>":
              for (n = i, delete n[0], o = 0; o < xr.length; ++o) n[xr[o]] && (n[xr[o]] = parseInt(n[xr[o]], 10));
              for (o = 0; o < Sr.length; ++o) n[Sr[o]] && (n[Sr[o]] = de(n[Sr[o]]));
              if (r.NumberFmt && n.numFmtId > 392) {
                for (o = 392; o > 60; --o) if (r.NumberFmt[n.numFmtId] == r.NumberFmt[o]) {
                  n.numFmtId = o;
                  break;
                }
              }
              r.CellXf.push(n);
              break;
            case "</xf>":
              break;
            case "<alignment":
            case "<alignment/>":
              var c = {};
              i.vertical && (c.vertical = i.vertical), i.horizontal && (c.horizontal = i.horizontal), i.textRotation != null && (c.textRotation = i.textRotation), i.indent && (c.indent = i.indent), i.wrapText && (c.wrapText = de(i.wrapText)), n.alignment = c;
              break;
            case "</alignment>":
              break;
            case "<protection":
              break;
            case "</protection>":
            case "<protection/>":
              break;
            case "<AlternateContent":
              a = !0;
              break;
            case "</AlternateContent>":
              a = !1;
              break;
            case "<extLst":
            case "<extLst>":
            case "</extLst>":
              break;
            case "<ext":
              a = !0;
              break;
            case "</ext>":
              a = !1;
              break;
            default:
              if (t && t.WTF && !a)
                throw new Error("unrecognized " + i[0] + " in cellXfs");
          }
        });
      }
      function Ko(e) {
        var r = [];
        return r[r.length] = X("cellXfs", null), e.forEach(function(t) {
          r[r.length] = X("xf", null, t);
        }), r[r.length] = "</cellXfs>", r.length === 2 ? "" : (r[0] = X("cellXfs", null, { count: r.length - 2 }).replace("/>", ">"), r.join(""));
      }
      var Zo = /* @__PURE__ */ (function() {
        var r = /<(?:\w+:)?numFmts([^>]*)>[\S\s]*?<\/(?:\w+:)?numFmts>/, t = /<(?:\w+:)?cellXfs([^>]*)>[\S\s]*?<\/(?:\w+:)?cellXfs>/, n = /<(?:\w+:)?fills([^>]*)>[\S\s]*?<\/(?:\w+:)?fills>/, a = /<(?:\w+:)?fonts([^>]*)>[\S\s]*?<\/(?:\w+:)?fonts>/, s = /<(?:\w+:)?borders([^>]*)>[\S\s]*?<\/(?:\w+:)?borders>/;
        return function(o, c, f) {
          var l = {};
          if (!o) return l;
          o = o.replace(/<!--([\s\S]*?)-->/gm, "").replace(/<!DOCTYPE[^\[]*\[[^\]]*\]>/gm, "");
          var p;
          return (p = o.match(r)) && Yo(p, l, f), (p = o.match(a)) && Go(p, l, c, f), (p = o.match(n)) && Xo(p, l, c, f), (p = o.match(s)) && Vo(p, l, c, f), (p = o.match(t)) && Jo(p, l, f), l;
        };
      })();
      function Wn(e, r) {
        var t = [we, X("styleSheet", null, { xmlns: bt[0], "xmlns:vt": xe.vt })], n;
        return e.SSF && (n = qo(e.SSF)) != null && (t[t.length] = n), t[t.length] = '<fonts count="1"><font><sz val="12"/><color theme="1"/><name val="Calibri"/><family val="2"/><scheme val="minor"/></font></fonts>', t[t.length] = '<fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills>', t[t.length] = '<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>', t[t.length] = '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>', (n = Ko(r.cellXfs)) && (t[t.length] = n), t[t.length] = '<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>', t[t.length] = '<dxfs count="0"/>', t[t.length] = '<tableStyles count="0" defaultTableStyle="TableStyleMedium9" defaultPivotStyle="PivotStyleMedium4"/>', t.length > 2 && (t[t.length] = "</styleSheet>", t[1] = t[1].replace("/>", ">")), t.join("");
      }
      var Qo = ["</a:lt1>", "</a:dk1>", "</a:lt2>", "</a:dk2>", "</a:accent1>", "</a:accent2>", "</a:accent3>", "</a:accent4>", "</a:accent5>", "</a:accent6>", "</a:hlink>", "</a:folHlink>"];
      function el(e, r, t) {
        r.themeElements.clrScheme = [];
        var n = {};
        (e[0].match(Ne) || []).forEach(function(a) {
          var s = K(a);
          switch (s[0]) {
            case "<a:clrScheme":
            case "</a:clrScheme>":
              break;
            case "<a:srgbClr":
              n.rgb = s.val;
              break;
            case "<a:sysClr":
              n.rgb = s.lastClr;
              break;
            case "<a:dk1>":
            case "</a:dk1>":
            case "<a:lt1>":
            case "</a:lt1>":
            case "<a:dk2>":
            case "</a:dk2>":
            case "<a:lt2>":
            case "</a:lt2>":
            case "<a:accent1>":
            case "</a:accent1>":
            case "<a:accent2>":
            case "</a:accent2>":
            case "<a:accent3>":
            case "</a:accent3>":
            case "<a:accent4>":
            case "</a:accent4>":
            case "<a:accent5>":
            case "</a:accent5>":
            case "<a:accent6>":
            case "</a:accent6>":
            case "<a:hlink>":
            case "</a:hlink>":
            case "<a:folHlink>":
            case "</a:folHlink>":
              s[0].charAt(1) === "/" ? (r.themeElements.clrScheme[Qo.indexOf(s[0])] = n, n = {}) : n.name = s[0].slice(3, s[0].length - 1);
              break;
            default:
              if (t && t.WTF) throw new Error("Unrecognized " + s[0] + " in clrScheme");
          }
        });
      }
      function tl() {
      }
      function rl() {
      }
      var al = /<a:clrScheme([^>]*)>[\s\S]*<\/a:clrScheme>/, nl = /<a:fontScheme([^>]*)>[\s\S]*<\/a:fontScheme>/, sl = /<a:fmtScheme([^>]*)>[\s\S]*<\/a:fmtScheme>/;
      function il(e, r, t) {
        r.themeElements = {};
        var n;
        [["clrScheme", al, el], ["fontScheme", nl, tl], ["fmtScheme", sl, rl]].forEach(function(a) {
          if (!(n = e.match(a[1]))) throw new Error(a[0] + " not found in themeElements");
          a[2](n, r, t);
        });
      }
      var ol = /<a:themeElements([^>]*)>[\s\S]*<\/a:themeElements>/;
      function ll(e, r) {
        (!e || e.length === 0) && (e = ca());
        var t, n = {};
        if (!(t = e.match(ol))) throw new Error("themeElements not found in theme");
        return il(t[0], n, r), n.raw = e, n;
      }
      function ca(e, r) {
        if (r && r.themeXLSX) return r.themeXLSX;
        if (e && typeof e.raw == "string") return e.raw;
        var t = [we];
        return t[t.length] = '<a:theme xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" name="Office Theme">', t[t.length] = "<a:themeElements>", t[t.length] = '<a:clrScheme name="Office">', t[t.length] = '<a:dk1><a:sysClr val="windowText" lastClr="000000"/></a:dk1>', t[t.length] = '<a:lt1><a:sysClr val="window" lastClr="FFFFFF"/></a:lt1>', t[t.length] = '<a:dk2><a:srgbClr val="1F497D"/></a:dk2>', t[t.length] = '<a:lt2><a:srgbClr val="EEECE1"/></a:lt2>', t[t.length] = '<a:accent1><a:srgbClr val="4F81BD"/></a:accent1>', t[t.length] = '<a:accent2><a:srgbClr val="C0504D"/></a:accent2>', t[t.length] = '<a:accent3><a:srgbClr val="9BBB59"/></a:accent3>', t[t.length] = '<a:accent4><a:srgbClr val="8064A2"/></a:accent4>', t[t.length] = '<a:accent5><a:srgbClr val="4BACC6"/></a:accent5>', t[t.length] = '<a:accent6><a:srgbClr val="F79646"/></a:accent6>', t[t.length] = '<a:hlink><a:srgbClr val="0000FF"/></a:hlink>', t[t.length] = '<a:folHlink><a:srgbClr val="800080"/></a:folHlink>', t[t.length] = "</a:clrScheme>", t[t.length] = '<a:fontScheme name="Office">', t[t.length] = "<a:majorFont>", t[t.length] = '<a:latin typeface="Cambria"/>', t[t.length] = '<a:ea typeface=""/>', t[t.length] = '<a:cs typeface=""/>', t[t.length] = '<a:font script="Jpan" typeface="ＭＳ Ｐゴシック"/>', t[t.length] = '<a:font script="Hang" typeface="맑은 고딕"/>', t[t.length] = '<a:font script="Hans" typeface="宋体"/>', t[t.length] = '<a:font script="Hant" typeface="新細明體"/>', t[t.length] = '<a:font script="Arab" typeface="Times New Roman"/>', t[t.length] = '<a:font script="Hebr" typeface="Times New Roman"/>', t[t.length] = '<a:font script="Thai" typeface="Tahoma"/>', t[t.length] = '<a:font script="Ethi" typeface="Nyala"/>', t[t.length] = '<a:font script="Beng" typeface="Vrinda"/>', t[t.length] = '<a:font script="Gujr" typeface="Shruti"/>', t[t.length] = '<a:font script="Khmr" typeface="MoolBoran"/>', t[t.length] = '<a:font script="Knda" typeface="Tunga"/>', t[t.length] = '<a:font script="Guru" typeface="Raavi"/>', t[t.length] = '<a:font script="Cans" typeface="Euphemia"/>', t[t.length] = '<a:font script="Cher" typeface="Plantagenet Cherokee"/>', t[t.length] = '<a:font script="Yiii" typeface="Microsoft Yi Baiti"/>', t[t.length] = '<a:font script="Tibt" typeface="Microsoft Himalaya"/>', t[t.length] = '<a:font script="Thaa" typeface="MV Boli"/>', t[t.length] = '<a:font script="Deva" typeface="Mangal"/>', t[t.length] = '<a:font script="Telu" typeface="Gautami"/>', t[t.length] = '<a:font script="Taml" typeface="Latha"/>', t[t.length] = '<a:font script="Syrc" typeface="Estrangelo Edessa"/>', t[t.length] = '<a:font script="Orya" typeface="Kalinga"/>', t[t.length] = '<a:font script="Mlym" typeface="Kartika"/>', t[t.length] = '<a:font script="Laoo" typeface="DokChampa"/>', t[t.length] = '<a:font script="Sinh" typeface="Iskoola Pota"/>', t[t.length] = '<a:font script="Mong" typeface="Mongolian Baiti"/>', t[t.length] = '<a:font script="Viet" typeface="Times New Roman"/>', t[t.length] = '<a:font script="Uigh" typeface="Microsoft Uighur"/>', t[t.length] = '<a:font script="Geor" typeface="Sylfaen"/>', t[t.length] = "</a:majorFont>", t[t.length] = "<a:minorFont>", t[t.length] = '<a:latin typeface="Calibri"/>', t[t.length] = '<a:ea typeface=""/>', t[t.length] = '<a:cs typeface=""/>', t[t.length] = '<a:font script="Jpan" typeface="ＭＳ Ｐゴシック"/>', t[t.length] = '<a:font script="Hang" typeface="맑은 고딕"/>', t[t.length] = '<a:font script="Hans" typeface="宋体"/>', t[t.length] = '<a:font script="Hant" typeface="新細明體"/>', t[t.length] = '<a:font script="Arab" typeface="Arial"/>', t[t.length] = '<a:font script="Hebr" typeface="Arial"/>', t[t.length] = '<a:font script="Thai" typeface="Tahoma"/>', t[t.length] = '<a:font script="Ethi" typeface="Nyala"/>', t[t.length] = '<a:font script="Beng" typeface="Vrinda"/>', t[t.length] = '<a:font script="Gujr" typeface="Shruti"/>', t[t.length] = '<a:font script="Khmr" typeface="DaunPenh"/>', t[t.length] = '<a:font script="Knda" typeface="Tunga"/>', t[t.length] = '<a:font script="Guru" typeface="Raavi"/>', t[t.length] = '<a:font script="Cans" typeface="Euphemia"/>', t[t.length] = '<a:font script="Cher" typeface="Plantagenet Cherokee"/>', t[t.length] = '<a:font script="Yiii" typeface="Microsoft Yi Baiti"/>', t[t.length] = '<a:font script="Tibt" typeface="Microsoft Himalaya"/>', t[t.length] = '<a:font script="Thaa" typeface="MV Boli"/>', t[t.length] = '<a:font script="Deva" typeface="Mangal"/>', t[t.length] = '<a:font script="Telu" typeface="Gautami"/>', t[t.length] = '<a:font script="Taml" typeface="Latha"/>', t[t.length] = '<a:font script="Syrc" typeface="Estrangelo Edessa"/>', t[t.length] = '<a:font script="Orya" typeface="Kalinga"/>', t[t.length] = '<a:font script="Mlym" typeface="Kartika"/>', t[t.length] = '<a:font script="Laoo" typeface="DokChampa"/>', t[t.length] = '<a:font script="Sinh" typeface="Iskoola Pota"/>', t[t.length] = '<a:font script="Mong" typeface="Mongolian Baiti"/>', t[t.length] = '<a:font script="Viet" typeface="Arial"/>', t[t.length] = '<a:font script="Uigh" typeface="Microsoft Uighur"/>', t[t.length] = '<a:font script="Geor" typeface="Sylfaen"/>', t[t.length] = "</a:minorFont>", t[t.length] = "</a:fontScheme>", t[t.length] = '<a:fmtScheme name="Office">', t[t.length] = "<a:fillStyleLst>", t[t.length] = '<a:solidFill><a:schemeClr val="phClr"/></a:solidFill>', t[t.length] = '<a:gradFill rotWithShape="1">', t[t.length] = "<a:gsLst>", t[t.length] = '<a:gs pos="0"><a:schemeClr val="phClr"><a:tint val="50000"/><a:satMod val="300000"/></a:schemeClr></a:gs>', t[t.length] = '<a:gs pos="35000"><a:schemeClr val="phClr"><a:tint val="37000"/><a:satMod val="300000"/></a:schemeClr></a:gs>', t[t.length] = '<a:gs pos="100000"><a:schemeClr val="phClr"><a:tint val="15000"/><a:satMod val="350000"/></a:schemeClr></a:gs>', t[t.length] = "</a:gsLst>", t[t.length] = '<a:lin ang="16200000" scaled="1"/>', t[t.length] = "</a:gradFill>", t[t.length] = '<a:gradFill rotWithShape="1">', t[t.length] = "<a:gsLst>", t[t.length] = '<a:gs pos="0"><a:schemeClr val="phClr"><a:tint val="100000"/><a:shade val="100000"/><a:satMod val="130000"/></a:schemeClr></a:gs>', t[t.length] = '<a:gs pos="100000"><a:schemeClr val="phClr"><a:tint val="50000"/><a:shade val="100000"/><a:satMod val="350000"/></a:schemeClr></a:gs>', t[t.length] = "</a:gsLst>", t[t.length] = '<a:lin ang="16200000" scaled="0"/>', t[t.length] = "</a:gradFill>", t[t.length] = "</a:fillStyleLst>", t[t.length] = "<a:lnStyleLst>", t[t.length] = '<a:ln w="9525" cap="flat" cmpd="sng" algn="ctr"><a:solidFill><a:schemeClr val="phClr"><a:shade val="95000"/><a:satMod val="105000"/></a:schemeClr></a:solidFill><a:prstDash val="solid"/></a:ln>', t[t.length] = '<a:ln w="25400" cap="flat" cmpd="sng" algn="ctr"><a:solidFill><a:schemeClr val="phClr"/></a:solidFill><a:prstDash val="solid"/></a:ln>', t[t.length] = '<a:ln w="38100" cap="flat" cmpd="sng" algn="ctr"><a:solidFill><a:schemeClr val="phClr"/></a:solidFill><a:prstDash val="solid"/></a:ln>', t[t.length] = "</a:lnStyleLst>", t[t.length] = "<a:effectStyleLst>", t[t.length] = "<a:effectStyle>", t[t.length] = "<a:effectLst>", t[t.length] = '<a:outerShdw blurRad="40000" dist="20000" dir="5400000" rotWithShape="0"><a:srgbClr val="000000"><a:alpha val="38000"/></a:srgbClr></a:outerShdw>', t[t.length] = "</a:effectLst>", t[t.length] = "</a:effectStyle>", t[t.length] = "<a:effectStyle>", t[t.length] = "<a:effectLst>", t[t.length] = '<a:outerShdw blurRad="40000" dist="23000" dir="5400000" rotWithShape="0"><a:srgbClr val="000000"><a:alpha val="35000"/></a:srgbClr></a:outerShdw>', t[t.length] = "</a:effectLst>", t[t.length] = "</a:effectStyle>", t[t.length] = "<a:effectStyle>", t[t.length] = "<a:effectLst>", t[t.length] = '<a:outerShdw blurRad="40000" dist="23000" dir="5400000" rotWithShape="0"><a:srgbClr val="000000"><a:alpha val="35000"/></a:srgbClr></a:outerShdw>', t[t.length] = "</a:effectLst>", t[t.length] = '<a:scene3d><a:camera prst="orthographicFront"><a:rot lat="0" lon="0" rev="0"/></a:camera><a:lightRig rig="threePt" dir="t"><a:rot lat="0" lon="0" rev="1200000"/></a:lightRig></a:scene3d>', t[t.length] = '<a:sp3d><a:bevelT w="63500" h="25400"/></a:sp3d>', t[t.length] = "</a:effectStyle>", t[t.length] = "</a:effectStyleLst>", t[t.length] = "<a:bgFillStyleLst>", t[t.length] = '<a:solidFill><a:schemeClr val="phClr"/></a:solidFill>', t[t.length] = '<a:gradFill rotWithShape="1">', t[t.length] = "<a:gsLst>", t[t.length] = '<a:gs pos="0"><a:schemeClr val="phClr"><a:tint val="40000"/><a:satMod val="350000"/></a:schemeClr></a:gs>', t[t.length] = '<a:gs pos="40000"><a:schemeClr val="phClr"><a:tint val="45000"/><a:shade val="99000"/><a:satMod val="350000"/></a:schemeClr></a:gs>', t[t.length] = '<a:gs pos="100000"><a:schemeClr val="phClr"><a:shade val="20000"/><a:satMod val="255000"/></a:schemeClr></a:gs>', t[t.length] = "</a:gsLst>", t[t.length] = '<a:path path="circle"><a:fillToRect l="50000" t="-80000" r="50000" b="180000"/></a:path>', t[t.length] = "</a:gradFill>", t[t.length] = '<a:gradFill rotWithShape="1">', t[t.length] = "<a:gsLst>", t[t.length] = '<a:gs pos="0"><a:schemeClr val="phClr"><a:tint val="80000"/><a:satMod val="300000"/></a:schemeClr></a:gs>', t[t.length] = '<a:gs pos="100000"><a:schemeClr val="phClr"><a:shade val="30000"/><a:satMod val="200000"/></a:schemeClr></a:gs>', t[t.length] = "</a:gsLst>", t[t.length] = '<a:path path="circle"><a:fillToRect l="50000" t="50000" r="50000" b="50000"/></a:path>', t[t.length] = "</a:gradFill>", t[t.length] = "</a:bgFillStyleLst>", t[t.length] = "</a:fmtScheme>", t[t.length] = "</a:themeElements>", t[t.length] = "<a:objectDefaults>", t[t.length] = "<a:spDef>", t[t.length] = '<a:spPr/><a:bodyPr/><a:lstStyle/><a:style><a:lnRef idx="1"><a:schemeClr val="accent1"/></a:lnRef><a:fillRef idx="3"><a:schemeClr val="accent1"/></a:fillRef><a:effectRef idx="2"><a:schemeClr val="accent1"/></a:effectRef><a:fontRef idx="minor"><a:schemeClr val="lt1"/></a:fontRef></a:style>', t[t.length] = "</a:spDef>", t[t.length] = "<a:lnDef>", t[t.length] = '<a:spPr/><a:bodyPr/><a:lstStyle/><a:style><a:lnRef idx="2"><a:schemeClr val="accent1"/></a:lnRef><a:fillRef idx="0"><a:schemeClr val="accent1"/></a:fillRef><a:effectRef idx="1"><a:schemeClr val="accent1"/></a:effectRef><a:fontRef idx="minor"><a:schemeClr val="tx1"/></a:fontRef></a:style>', t[t.length] = "</a:lnDef>", t[t.length] = "</a:objectDefaults>", t[t.length] = "<a:extraClrSchemeLst/>", t[t.length] = "</a:theme>", t.join("");
      }
      function cl(e, r, t) {
        var n = { Types: [], Cell: [], Value: [] };
        if (!e) return n;
        var a = !1, s = 2, i;
        return e.replace(Ne, function(o) {
          var c = K(o);
          switch (rt(c[0])) {
            case "<?xml":
              break;
            case "<metadata":
            case "</metadata>":
              break;
            case "<metadataTypes":
            case "</metadataTypes>":
              break;
            case "<metadataType":
              n.Types.push({ name: c.name });
              break;
            case "</metadataType>":
              break;
            case "<futureMetadata":
              for (var f = 0; f < n.Types.length; ++f) n.Types[f].name == c.name && (i = n.Types[f]);
              break;
            case "</futureMetadata>":
              break;
            case "<bk>":
              break;
            case "</bk>":
              break;
            case "<rc":
              s == 1 ? n.Cell.push({ type: n.Types[c.t - 1].name, index: +c.v }) : s == 0 && n.Value.push({ type: n.Types[c.t - 1].name, index: +c.v });
              break;
            case "</rc>":
              break;
            case "<cellMetadata":
              s = 1;
              break;
            case "</cellMetadata>":
              s = 2;
              break;
            case "<valueMetadata":
              s = 0;
              break;
            case "</valueMetadata>":
              s = 2;
              break;
            case "<extLst":
            case "<extLst>":
            case "</extLst>":
            case "<extLst/>":
              break;
            case "<ext":
              a = !0;
              break;
            case "</ext>":
              a = !1;
              break;
            case "<rvb":
              if (!i) break;
              i.offsets || (i.offsets = []), i.offsets.push(+c.i);
              break;
            default:
              if (!a && t.WTF) throw new Error("unrecognized " + c[0] + " in metadata");
          }
          return o;
        }), n;
      }
      function In() {
        var e = [we];
        return e.push(`<metadata xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:xlrd="http://schemas.microsoft.com/office/spreadsheetml/2017/richdata" xmlns:xda="http://schemas.microsoft.com/office/spreadsheetml/2017/dynamicarray">
  <metadataTypes count="1">
    <metadataType name="XLDAPR" minSupportedVersion="120000" copy="1" pasteAll="1" pasteValues="1" merge="1" splitFirst="1" rowColShift="1" clearFormats="1" clearComments="1" assign="1" coerce="1" cellMeta="1"/>
  </metadataTypes>
  <futureMetadata name="XLDAPR" count="1">
    <bk>
      <extLst>
        <ext uri="{bdbb8cdc-fa1e-496e-a857-3c3f30c029c3}">
          <xda:dynamicArrayProperties fDynamic="1" fCollapsed="0"/>
        </ext>
      </extLst>
    </bk>
  </futureMetadata>
  <cellMetadata count="1">
    <bk>
      <rc t="1" v="0"/>
    </bk>
  </cellMetadata>
</metadata>`), e.join("");
      }
      function Gf() {
      }
      function fl(e, r, t, n) {
        if (!e) return e;
        var a = n || {}, s = !1;
        ao(e, function(o, c, f) {
          switch (f) {
            case 359:
            case 363:
            case 364:
            case 366:
            case 367:
            case 368:
            case 369:
            case 370:
            case 371:
            case 472:
            case 577:
            case 578:
            case 579:
            case 580:
            case 581:
            case 582:
            case 583:
            case 584:
            case 585:
            case 586:
            case 587:
              break;
            case 35:
              s = !0;
              break;
            case 36:
              s = !1;
              break;
            default:
              if (!c.T) {
                if (!s || a.WTF) throw new Error("Unexpected record 0x" + f.toString(16));
              }
          }
        }, a);
      }
      function ul(e, r) {
        if (!e) return "??";
        var t = (e.match(/<c:chart [^>]*r:id="([^"]*)"/) || ["", ""])[1];
        return r["!id"][t].Target;
      }
      var Rt = 1024;
      function Ln(e, r) {
        for (var t = [21600, 21600], n = ["m0,0l0", t[1], t[0], t[1], t[0], "0xe"].join(","), a = [X("xml", null, { "xmlns:v": gr.v, "xmlns:o": gr.o, "xmlns:x": gr.x, "xmlns:mv": gr.mv }).replace(/\/>/, ">"), X("o:shapelayout", X("o:idmap", null, { "v:ext": "edit", data: e }), { "v:ext": "edit" }), X("v:shapetype", [X("v:stroke", null, { joinstyle: "miter" }), X("v:path", null, { gradientshapeok: "t", "o:connecttype": "rect" })].join(""), { id: "_x0000_t202", "o:spt": 202, coordsize: t.join(","), path: n })]; Rt < e * 1e3; ) Rt += 1e3;
        return r.forEach(function(s) {
          var i = Fe(s[0]), o = { color2: "#BEFF82", type: "gradient" };
          o.type == "gradient" && (o.angle = "-180");
          var c = o.type == "gradient" ? X("o:fill", null, { type: "gradientUnscaled", "v:ext": "view" }) : null, f = X("v:fill", c, o), l = { on: "t", obscured: "t" };
          ++Rt, a = a.concat(["<v:shape" + qt({ id: "_x0000_s" + Rt, type: "#_x0000_t202", style: "position:absolute; margin-left:80pt;margin-top:5pt;width:104pt;height:64pt;z-index:10" + (s[1].hidden ? ";visibility:hidden" : ""), fillcolor: "#ECFAD4", strokecolor: "#edeaa1" }) + ">", f, X("v:shadow", null, l), X("v:path", null, { "o:connecttype": "none" }), '<v:textbox><div style="text-align:left"></div></v:textbox>', '<x:ClientData ObjectType="Note">', "<x:MoveWithCells/>", "<x:SizeWithCells/>", Ve("x:Anchor", [i.c + 1, 0, i.r + 1, 0, i.c + 3, 20, i.r + 5, 20].join(",")), Ve("x:AutoFill", "False"), Ve("x:Row", String(i.r)), Ve("x:Column", String(i.c)), s[1].hidden ? "" : "<x:Visible/>", "</x:ClientData>", "</v:shape>"]);
        }), a.push("</xml>"), a.join("");
      }
      function $n(e, r, t, n) {
        var a = Array.isArray(e), s;
        r.forEach(function(i) {
          var o = Fe(i.ref);
          if (a ? (e[o.r] || (e[o.r] = []), s = e[o.r][o.c]) : s = e[i.ref], !s) {
            s = { t: "z" }, a ? e[o.r][o.c] = s : e[i.ref] = s;
            var c = ke(e["!ref"] || "BDWGO1000001:A1");
            c.s.r > o.r && (c.s.r = o.r), c.e.r < o.r && (c.e.r = o.r), c.s.c > o.c && (c.s.c = o.c), c.e.c < o.c && (c.e.c = o.c);
            var f = ye(c);
            f !== e["!ref"] && (e["!ref"] = f);
          }
          s.c || (s.c = []);
          var l = { a: i.author, t: i.t, r: i.r, T: t };
          i.h && (l.h = i.h);
          for (var p = s.c.length - 1; p >= 0; --p) {
            if (!t && s.c[p].T) return;
            t && !s.c[p].T && s.c.splice(p, 1);
          }
          if (t && n) {
            for (p = 0; p < n.length; ++p)
              if (l.a == n[p].id) {
                l.a = n[p].name || l.a;
                break;
              }
          }
          s.c.push(l);
        });
      }
      function hl(e, r) {
        if (e.match(/<(?:\w+:)?comments *\/>/)) return [];
        var t = [], n = [], a = e.match(/<(?:\w+:)?authors>([\s\S]*)<\/(?:\w+:)?authors>/);
        a && a[1] && a[1].split(/<\/\w*:?author>/).forEach(function(i) {
          if (!(i === "" || i.trim() === "")) {
            var o = i.match(/<(?:\w+:)?author[^>]*>(.*)/);
            o && t.push(o[1]);
          }
        });
        var s = e.match(/<(?:\w+:)?commentList>([\s\S]*)<\/(?:\w+:)?commentList>/);
        return s && s[1] && s[1].split(/<\/\w*:?comment>/).forEach(function(i) {
          if (!(i === "" || i.trim() === "")) {
            var o = i.match(/<(?:\w+:)?comment[^>]*>/);
            if (o) {
              var c = K(o[0]), f = { author: c.authorId && t[c.authorId] || "sheetjsghost", ref: c.ref, guid: c.guid }, l = Fe(c.ref);
              if (!(r.sheetRows && r.sheetRows <= l.r)) {
                var p = i.match(/<(?:\w+:)?text>([\s\S]*)<\/(?:\w+:)?text>/), d = !!p && !!p[1] && aa(p[1]) || { r: "", t: "", h: "" };
                f.r = d.r, d.r == "<t></t>" && (d.t = d.h = ""), f.t = (d.t || "").replace(/\r\n/g, `
`).replace(/\r/g, `
`), r.cellHTML && (f.h = d.h), n.push(f);
              }
            }
          }
        }), n;
      }
      function zn(e) {
        var r = [we, X("comments", null, { xmlns: bt[0] })], t = [];
        return r.push("<authors>"), e.forEach(function(n) {
          n[1].forEach(function(a) {
            var s = he(a.a);
            t.indexOf(s) == -1 && (t.push(s), r.push("<author>" + s + "</author>")), a.T && a.ID && t.indexOf("tc=" + a.ID) == -1 && (t.push("tc=" + a.ID), r.push("<author>tc=" + a.ID + "</author>"));
          });
        }), t.length == 0 && (t.push("SheetJ5"), r.push("<author>SheetJ5</author>")), r.push("</authors>"), r.push("<commentList>"), e.forEach(function(n) {
          var a = 0, s = [];
          if (n[1][0] && n[1][0].T && n[1][0].ID ? a = t.indexOf("tc=" + n[1][0].ID) : n[1].forEach(function(c) {
            c.a && (a = t.indexOf(he(c.a))), s.push(c.t || "");
          }), r.push('<comment ref="' + n[0] + '" authorId="' + a + '"><text>'), s.length <= 1) r.push(Ve("t", he(s[0] || "")));
          else {
            for (var i = `Comment:
    ` + s[0] + `
`, o = 1; o < s.length; ++o) i += `Reply:
    ` + s[o] + `
`;
            r.push(Ve("t", he(i)));
          }
          r.push("</text></comment>");
        }), r.push("</commentList>"), r.length > 2 && (r[r.length] = "</comments>", r[1] = r[1].replace("/>", ">")), r.join("");
      }
      function pl(e, r) {
        var t = [], n = !1, a = {}, s = 0;
        return e.replace(Ne, function(o, c) {
          var f = K(o);
          switch (rt(f[0])) {
            case "<?xml":
              break;
            case "<ThreadedComments":
              break;
            case "</ThreadedComments>":
              break;
            case "<threadedComment":
              a = { author: f.personId, guid: f.id, ref: f.ref, T: 1 };
              break;
            case "</threadedComment>":
              a.t != null && t.push(a);
              break;
            case "<text>":
            case "<text":
              s = c + o.length;
              break;
            case "</text>":
              a.t = e.slice(s, c).replace(/\r\n/g, `
`).replace(/\r/g, `
`);
              break;
            case "<mentions":
            case "<mentions>":
              n = !0;
              break;
            case "</mentions>":
              n = !1;
              break;
            case "<extLst":
            case "<extLst>":
            case "</extLst>":
            case "<extLst/>":
              break;
            case "<ext":
              n = !0;
              break;
            case "</ext>":
              n = !1;
              break;
            default:
              if (!n && r.WTF) throw new Error("unrecognized " + f[0] + " in threaded comments");
          }
          return o;
        }), t;
      }
      function ml(e, r, t) {
        var n = [we, X("ThreadedComments", null, { xmlns: xe.TCMNT }).replace(/[\/]>/, ">")];
        return e.forEach(function(a) {
          var s = "";
          (a[1] || []).forEach(function(i, o) {
            if (!i.T) {
              delete i.ID;
              return;
            }
            i.a && r.indexOf(i.a) == -1 && r.push(i.a);
            var c = { ref: a[0], id: "{54EE7951-7262-4200-6969-" + ("000000000000" + t.tcid++).slice(-12) + "}" };
            o == 0 ? s = c.id : c.parentId = s, i.ID = c.id, i.a && (c.personId = "{54EE7950-7262-4200-6969-" + ("000000000000" + r.indexOf(i.a)).slice(-12) + "}"), n.push(X("threadedComment", Ve("text", i.t || ""), c));
          });
        }), n.push("</ThreadedComments>"), n.join("");
      }
      function dl(e, r) {
        var t = [], n = !1;
        return e.replace(Ne, function(s) {
          var i = K(s);
          switch (rt(i[0])) {
            case "<?xml":
              break;
            case "<personList":
              break;
            case "</personList>":
              break;
            case "<person":
              t.push({ name: i.displayname, id: i.id });
              break;
            case "</person>":
              break;
            case "<extLst":
            case "<extLst>":
            case "</extLst>":
            case "<extLst/>":
              break;
            case "<ext":
              n = !0;
              break;
            case "</ext>":
              n = !1;
              break;
            default:
              if (!n && r.WTF) throw new Error("unrecognized " + i[0] + " in threaded comments");
          }
          return s;
        }), t;
      }
      function vl(e) {
        var r = [we, X("personList", null, { xmlns: xe.TCMNT, "xmlns:x": bt[0] }).replace(/[\/]>/, ">")];
        return e.forEach(function(t, n) {
          r.push(X("person", null, { displayName: t, id: "{54EE7950-7262-4200-6969-" + ("000000000000" + n).slice(-12) + "}", userId: t, providerId: "None" }));
        }), r.push("</personList>"), r.join("");
      }
      var gl = "application/vnd.ms-office.vbaProject", jn = ["xlsb", "xlsm", "xlam", "biff8", "xla"];
      function bl() {
        return { "!type": "dialog" };
      }
      function wl() {
        return { "!type": "dialog" };
      }
      function yl() {
        return { "!type": "macro" };
      }
      function kl() {
        return { "!type": "macro" };
      }
      var xl = /* @__PURE__ */ (function() {
        var e = /(^|[^A-Za-z_])R(\[?-?\d+\]|[1-9]\d*|)C(\[?-?\d+\]|[1-9]\d*|)(?![A-Za-z0-9_])/g, r = { r: 0, c: 0 };
        function t(n, a, s, i) {
          var o = !1, c = !1;
          s.length == 0 ? c = !0 : s.charAt(0) == "[" && (c = !0, s = s.slice(1, -1)), i.length == 0 ? o = !0 : i.charAt(0) == "[" && (o = !0, i = i.slice(1, -1));
          var f = s.length > 0 ? parseInt(s, 10) | 0 : 0, l = i.length > 0 ? parseInt(i, 10) | 0 : 0;
          return o ? l += r.c : --l, c ? f += r.r : --f, a + (o ? "" : "$") + at(l) + (c ? "" : "$") + qe(f);
        }
        return function(a, s) {
          return r = s, a.replace(e, t);
        };
      })(), fa = /(^|[^._A-Z0-9])([$]?)([A-Z]{1,2}|[A-W][A-Z]{2}|X[A-E][A-Z]|XF[A-D])([$]?)(10[0-3]\d{4}|104[0-7]\d{3}|1048[0-4]\d{2}|10485[0-6]\d|104857[0-6]|[1-9]\d{0,5})(?![_.\(A-Za-z0-9])/g, Sl = /* @__PURE__ */ (function() {
        return function(r, t) {
          return r.replace(fa, function(n, a, s, i, o, c) {
            var f = Jr(i) - (s ? 0 : t.c), l = qr(c) - (o ? 0 : t.r), p = l == 0 ? "" : o ? l + 1 : "[" + l + "]", d = f == 0 ? "" : s ? f + 1 : "[" + f + "]";
            return a + "R" + p + "C" + d;
          });
        };
      })();
      function Un(e, r) {
        return e.replace(fa, function(t, n, a, s, i, o) {
          return n + (a == "$" ? a + s : at(Jr(s) + r.c)) + (i == "$" ? i + o : qe(qr(o) + r.r));
        });
      }
      function Cl(e, r, t) {
        var n = nt(r), a = n.s, s = Fe(t), i = { r: s.r - a.r, c: s.c - a.c };
        return Un(e, i);
      }
      function _l(e) {
        return e.length != 1;
      }
      function Bn(e) {
        return e.replace(/_xlfn\./g, "");
      }
      function Hn(e) {
        return e.slice(0, 3) == "of:" && (e = e.slice(3)), e.charCodeAt(0) == 61 && (e = e.slice(1), e.charCodeAt(0) == 61 && (e = e.slice(1))), e = e.replace(/COM\.MICROSOFT\./g, ""), e = e.replace(/\[((?:\.[A-Z]+[0-9]+)(?::\.[A-Z]+[0-9]+)?)\]/g, function(r, t) {
          return t.replace(/\./g, "");
        }), e = e.replace(/\[.(#[A-Z]*[?!])\]/g, "$1"), e.replace(/[;~]/g, ",").replace(/\|/g, ";");
      }
      function Al(e) {
        var r = "of:=" + e.replace(fa, "$1[.$2$3$4$5]").replace(/\]:\[/g, ":");
        return r.replace(/;/g, "|").replace(/,/g, ";");
      }
      function ua(e) {
        var r = e.split(":"), t = r[0].split(".")[0];
        return [t, r[0].split(".")[1] + (r.length > 1 ? ":" + (r[1].split(".")[1] || r[1].split(".")[0]) : "")];
      }
      function Fl(e) {
        return e.replace(/\./, "!");
      }
      var Cr = {}, Nt = {}, rr = typeof Map < "u";
      function Tl(e, r, t) {
        var n = 0, a = e.length;
        if (t) {
          if (rr ? t.has(r) : Object.prototype.hasOwnProperty.call(t, r)) {
            for (var s = rr ? t.get(r) : t[r]; n < s.length; ++n)
              if (e[s[n]].t === r)
                return e.Count++, s[n];
          }
        } else for (; n < a; ++n)
          if (e[n].t === r)
            return e.Count++, n;
        return e[a] = { t: r }, e.Count++, e.Unique++, t && (rr ? (t.has(r) || t.set(r, []), t.get(r).push(a)) : (Object.prototype.hasOwnProperty.call(t, r) || (t[r] = []), t[r].push(a))), a;
      }
      function El(e, r) {
        var t = { min: e + 1, max: e + 1 }, n = -1;
        return r.MDW && (Me = r.MDW), r.width != null ? t.customWidth = 1 : r.wpx != null ? n = tr(r.wpx) : r.wch != null && (n = r.wch), n > -1 ? (t.width = kr(n), t.customWidth = 1) : r.width != null && (t.width = r.width), r.hidden && (t.hidden = !0), r.level != null && (t.outlineLevel = t.level = r.level), t;
      }
      function Dl(e, r) {
        if (e) {
          var t = [0.7, 0.7, 0.75, 0.75, 0.3, 0.3];
          e.left == null && (e.left = t[0]), e.right == null && (e.right = t[1]), e.top == null && (e.top = t[2]), e.bottom == null && (e.bottom = t[3]), e.header == null && (e.header = t[4]), e.footer == null && (e.footer = t[5]);
        }
      }
      function ha(e, r, t) {
        var n = t.revssf[r.z != null ? r.z : "General"], a = 60, s = e.length;
        if (n == null && t.ssf) {
          for (; a < 392; ++a) if (t.ssf[a] == null) {
            Ht(r.z, a), t.ssf[a] = r.z, t.revssf[r.z] = n = a;
            break;
          }
        }
        for (a = 0; a != s; ++a) if (e[a].numFmtId === n) return a;
        return e[s] = { numFmtId: n, fontId: 0, fillId: 0, borderId: 0, xfId: 0, applyNumberFormat: 1 }, s;
      }
      function Ol(e, r, t, n, a, s) {
        try {
          n.cellNF && (e.z = ce[r]);
        } catch (o) {
          if (n.WTF) throw o;
        }
        if (!(e.t === "z" && !n.cellStyles)) {
          if (e.t === "d" && typeof e.v == "string" && (e.v = Se(e.v)), (!n || n.cellText !== !1) && e.t !== "z") try {
            if (ce[r] == null && Ht(hi[r] || "General", r), e.t === "e") e.w = e.w || Zr[e.v];
            else if (r === 0)
              if (e.t === "n")
                (e.v | 0) === e.v ? e.w = e.v.toString(10) : e.w = zr(e.v);
              else if (e.t === "d") {
                var i = Oe(e.v);
                (i | 0) === i ? e.w = i.toString(10) : e.w = zr(i);
              } else {
                if (e.v === void 0) return "";
                e.w = mr(e.v, Nt);
              }
            else e.t === "d" ? e.w = He(r, Oe(e.v), Nt) : e.w = He(r, e.v, Nt);
          } catch (o) {
            if (n.WTF) throw o;
          }
          if (n.cellStyles && t != null)
            try {
              e.s = s.Fills[t], e.s.fgColor && e.s.fgColor.theme && !e.s.fgColor.rgb && (e.s.fgColor.rgb = na(a.themeElements.clrScheme[e.s.fgColor.theme].rgb, e.s.fgColor.tint || 0), n.WTF && (e.s.fgColor.raw_rgb = a.themeElements.clrScheme[e.s.fgColor.theme].rgb)), e.s.bgColor && e.s.bgColor.theme && (e.s.bgColor.rgb = na(a.themeElements.clrScheme[e.s.bgColor.theme].rgb, e.s.bgColor.tint || 0), n.WTF && (e.s.bgColor.raw_rgb = a.themeElements.clrScheme[e.s.bgColor.theme].rgb));
            } catch (o) {
              if (n.WTF && s.Fills) throw o;
            }
        }
      }
      function Ml(e, r, t) {
        if (e && e["!ref"]) {
          var n = ke(e["!ref"]);
          if (n.e.c < n.s.c || n.e.r < n.s.r) throw new Error("Bad range (" + t + "): " + e["!ref"]);
        }
      }
      function Rl(e, r) {
        var t = ke(r);
        t.s.r <= t.e.r && t.s.c <= t.e.c && t.s.r >= 0 && t.s.c >= 0 && (e["!ref"] = ye(t));
      }
      var Nl = /<(?:\w:)?mergeCell ref="[A-Z0-9:]+"\s*[\/]?>/g, Pl = /<(?:\w+:)?sheetData[^>]*>([\s\S]*)<\/(?:\w+:)?sheetData>/, Wl = /<(?:\w:)?hyperlink [^>]*>/gm, Il = /"(\w*:\w*)"/, Ll = /<(?:\w:)?col\b[^>]*[\/]?>/g, $l = /<(?:\w:)?autoFilter[^>]*([\/]|>([\s\S]*)<\/(?:\w:)?autoFilter)>/g, zl = /<(?:\w:)?pageMargins[^>]*\/>/g, Vn = /<(?:\w:)?sheetPr\b(?:[^>a-z][^>]*)?\/>/, jl = /<(?:\w:)?sheetPr[^>]*(?:[\/]|>([\s\S]*)<\/(?:\w:)?sheetPr)>/, Ul = /<(?:\w:)?sheetViews[^>]*(?:[\/]|>([\s\S]*)<\/(?:\w:)?sheetViews)>/;
      function Bl(e, r, t, n, a, s, i) {
        if (!e) return e;
        n || (n = { "!id": {} });
        var o = r.dense ? [] : {}, c = { s: { r: 2e6, c: 2e6 }, e: { r: 0, c: 0 } }, f = "", l = "", p = e.match(Pl);
        p ? (f = e.slice(0, p.index), l = e.slice(p.index + p[0].length)) : f = l = e;
        var d = f.match(Vn);
        d ? pa(d[0], o, a, t) : (d = f.match(jl)) && Vl(d[0], d[1] || "", o, a, t);
        var m = (f.match(/<(?:\w*:)?dimension/) || { index: -1 }).index;
        if (m > 0) {
          var w = f.slice(m, m + 50).match(Il);
          w && Rl(o, w[1]);
        }
        var u = f.match(Ul);
        u && u[1] && nc(u[1], a);
        var g = [];
        if (r.cellStyles) {
          var C = f.match(Ll);
          C && Ql(g, C);
        }
        p && oc(p[1], o, r, c, s, i);
        var _ = l.match($l);
        _ && (o["!autofilter"] = tc(_[0]));
        var E = [], R = l.match(Nl);
        if (R) for (m = 0; m != R.length; ++m) E[m] = ke(R[m].slice(R[m].indexOf('"') + 1));
        var N = l.match(Wl);
        N && Jl(o, N, n);
        var F = l.match(zl);
        if (F && (o["!margins"] = Kl(K(F[0]))), !o["!ref"] && c.e.c >= c.s.c && c.e.r >= c.s.r && (o["!ref"] = ye(c)), r.sheetRows > 0 && o["!ref"]) {
          var S = ke(o["!ref"]);
          r.sheetRows <= +S.e.r && (S.e.r = r.sheetRows - 1, S.e.r > c.e.r && (S.e.r = c.e.r), S.e.r < S.s.r && (S.s.r = S.e.r), S.e.c > c.e.c && (S.e.c = c.e.c), S.e.c < S.s.c && (S.s.c = S.e.c), o["!fullref"] = o["!ref"], o["!ref"] = ye(S));
        }
        return g.length > 0 && (o["!cols"] = g), E.length > 0 && (o["!merges"] = E), o;
      }
      function Hl(e) {
        if (e.length === 0) return "";
        for (var r = '<mergeCells count="' + e.length + '">', t = 0; t != e.length; ++t) r += '<mergeCell ref="' + ye(e[t]) + '"/>';
        return r + "</mergeCells>";
      }
      function pa(e, r, t, n) {
        var a = K(e);
        t.Sheets[n] || (t.Sheets[n] = {}), a.codeName && (t.Sheets[n].CodeName = fe(ie(a.codeName)));
      }
      function Vl(e, r, t, n, a) {
        pa(e.slice(0, e.indexOf(">")), t, n, a);
      }
      function Xl(e, r, t, n, a) {
        var s = !1, i = {}, o = null;
        if (n.bookType !== "xlsx" && r.vbaraw) {
          var c = r.SheetNames[t];
          try {
            r.Workbook && (c = r.Workbook.Sheets[t].CodeName || c);
          } catch {
          }
          s = !0, i.codeName = gt(he(c));
        }
        if (e && e["!outline"]) {
          var f = { summaryBelow: 1, summaryRight: 1 };
          e["!outline"].above && (f.summaryBelow = 0), e["!outline"].left && (f.summaryRight = 0), o = (o || "") + X("outlinePr", null, f);
        }
        !s && !o || (a[a.length] = X("sheetPr", o, i));
      }
      var Gl = ["objects", "scenarios", "selectLockedCells", "selectUnlockedCells"], Yl = ["formatColumns", "formatRows", "formatCells", "insertColumns", "insertRows", "insertHyperlinks", "deleteColumns", "deleteRows", "sort", "autoFilter", "pivotTables"];
      function ql(e) {
        var r = { sheet: 1 };
        return Gl.forEach(function(t) {
          e[t] != null && e[t] && (r[t] = "1");
        }), Yl.forEach(function(t) {
          e[t] != null && !e[t] && (r[t] = "0");
        }), e.password && (r.password = crypto_CreatePasswordVerifier_Method1(e.password).toString(16).toUpperCase()), X("sheetProtection", null, r);
      }
      function Jl(e, r, t) {
        for (var n = Array.isArray(e), a = 0; a != r.length; ++a) {
          var s = K(ie(r[a]), !0);
          if (!s.ref) return;
          var i = ((t || {})["!id"] || [])[s.id];
          i ? (s.Target = i.Target, s.location && (s.Target += "#" + fe(s.location))) : (s.Target = "#" + fe(s.location), i = { Target: s.Target, TargetMode: "Internal" }), s.Rel = i, s.tooltip && (s.Tooltip = s.tooltip, delete s.tooltip);
          for (var o = ke(s.ref), c = o.s.r; c <= o.e.r; ++c) for (var f = o.s.c; f <= o.e.c; ++f) {
            var l = ge({ c: f, r: c });
            n ? (e[c] || (e[c] = []), e[c][f] || (e[c][f] = { t: "z", v: void 0 }), e[c][f].l = s) : (e[l] || (e[l] = { t: "z", v: void 0 }), e[l].l = s);
          }
        }
      }
      function Kl(e) {
        var r = {};
        return ["left", "right", "top", "bottom", "header", "footer"].forEach(function(t) {
          e[t] && (r[t] = parseFloat(e[t]));
        }), r;
      }
      function Zl(e) {
        return Dl(e), X("pageMargins", null, e);
      }
      function Ql(e, r) {
        for (var t = !1, n = 0; n != r.length; ++n) {
          var a = K(r[n], !0);
          a.hidden && (a.hidden = de(a.hidden));
          var s = parseInt(a.min, 10) - 1, i = parseInt(a.max, 10) - 1;
          for (a.outlineLevel && (a.level = +a.outlineLevel || 0), delete a.min, delete a.max, a.width = +a.width, !t && a.width && (t = !0, Bo(a.width)), ia(a); s <= i; ) e[s++] = Ae(a);
        }
      }
      function ec(e, r) {
        for (var t = ["<cols>"], n, a = 0; a != r.length; ++a)
          (n = r[a]) && (t[t.length] = X("col", null, El(a, n)));
        return t[t.length] = "</cols>", t.join("");
      }
      function tc(e) {
        var r = { ref: (e.match(/ref="([^"]*)"/) || [])[1] };
        return r;
      }
      function rc(e, r, t, n) {
        var a = typeof e.ref == "string" ? e.ref : ye(e.ref);
        t.Workbook || (t.Workbook = { Sheets: [] }), t.Workbook.Names || (t.Workbook.Names = []);
        var s = t.Workbook.Names, i = nt(a);
        i.s.r == i.e.r && (i.e.r = nt(r["!ref"]).e.r, a = ye(i));
        for (var o = 0; o < s.length; ++o) {
          var c = s[o];
          if (c.Name == "_xlnm._FilterDatabase" && c.Sheet == n) {
            c.Ref = "'" + t.SheetNames[n] + "'!" + a;
            break;
          }
        }
        return o == s.length && s.push({ Name: "_xlnm._FilterDatabase", Sheet: n, Ref: "'" + t.SheetNames[n] + "'!" + a }), X("autoFilter", null, { ref: a });
      }
      var ac = /<(?:\w:)?sheetView(?:[^>a-z][^>]*)?\/?>/;
      function nc(e, r) {
        r.Views || (r.Views = [{}]), (e.match(ac) || []).forEach(function(t, n) {
          var a = K(t);
          r.Views[n] || (r.Views[n] = {}), +a.zoomScale && (r.Views[n].zoom = +a.zoomScale), de(a.rightToLeft) && (r.Views[n].RTL = !0);
        });
      }
      function sc(e, r, t, n) {
        var a = { workbookViewId: "0" };
        return (((n || {}).Workbook || {}).Views || [])[0] && (a.rightToLeft = n.Workbook.Views[0].RTL ? "1" : "0"), X("sheetViews", X("sheetView", null, a), {});
      }
      function ic(e, r, t, n) {
        if (e.c && t["!comments"].push([r, e.c]), e.v === void 0 && typeof e.f != "string" || e.t === "z" && !e.f) return "";
        var a = "", s = e.t, i = e.v;
        if (e.t !== "z") switch (e.t) {
          case "b":
            a = e.v ? "1" : "0";
            break;
          case "n":
            a = "" + e.v;
            break;
          case "e":
            a = Zr[e.v];
            break;
          case "d":
            n && n.cellDates ? a = Se(e.v, -1).toISOString() : (e = Ae(e), e.t = "n", a = "" + (e.v = Oe(Se(e.v)))), typeof e.z > "u" && (e.z = ce[14]);
            break;
          default:
            a = e.v;
            break;
        }
        var o = Ve("v", he(a)), c = { r }, f = ha(n.cellXfs, e, n);
        switch (f !== 0 && (c.s = f), e.t) {
          case "n":
            break;
          case "d":
            c.t = "d";
            break;
          case "b":
            c.t = "b";
            break;
          case "e":
            c.t = "e";
            break;
          case "z":
            break;
          default:
            if (e.v == null) {
              delete e.t;
              break;
            }
            if (e.v.length > 32767) throw new Error("Text length must not exceed 32767 characters");
            if (n && n.bookSST) {
              o = Ve("v", "" + Tl(n.Strings, e.v, n.revStrings)), c.t = "s";
              break;
            }
            c.t = "str";
            break;
        }
        if (e.t != s && (e.t = s, e.v = i), typeof e.f == "string" && e.f) {
          var l = e.F && e.F.slice(0, r.length) == r ? { t: "array", ref: e.F } : null;
          o = X("f", he(e.f), l) + (e.v != null ? o : "");
        }
        return e.l && t["!links"].push([r, e.l]), e.D && (c.cm = 1), X("c", o, c);
      }
      var oc = (function() {
        var e = /<(?:\w+:)?c[ \/>]/, r = /<\/(?:\w+:)?row>/, t = /r=["']([^"']*)["']/, n = /<(?:\w+:)?is>([\S\s]*?)<\/(?:\w+:)?is>/, a = /ref=["']([^"']*)["']/, s = Yt("v"), i = Yt("f");
        return function(c, f, l, p, d, m) {
          for (var w = 0, u = "", g = [], C = [], _ = 0, E = 0, R = 0, N = "", F, S, W = 0, M = 0, L, I, z = 0, G = 0, Q = Array.isArray(m.CellXf), Y, ee = [], ue = [], st = Array.isArray(f), ae = [], te = {}, oe = !1, Pe = !!l.sheetStubs, je = c.split(r), Ue = 0, Ke = je.length; Ue != Ke; ++Ue) {
            u = je[Ue].trim();
            var Xe = u.length;
            if (Xe !== 0) {
              var ve = 0;
              e: for (w = 0; w < Xe; ++w) switch (u[w]) {
                case ">":
                  if (u[w - 1] != "/") {
                    ++w;
                    break e;
                  }
                  if (l && l.cellStyles) {
                    if (S = K(u.slice(ve, w), !0), W = S.r != null ? parseInt(S.r, 10) : W + 1, M = -1, l.sheetRows && l.sheetRows < W) continue;
                    te = {}, oe = !1, S.ht && (oe = !0, te.hpt = parseFloat(S.ht), te.hpx = la(te.hpt)), S.hidden == "1" && (oe = !0, te.hidden = !0), S.outlineLevel != null && (oe = !0, te.level = +S.outlineLevel), oe && (ae[W - 1] = te);
                  }
                  break;
                case "<":
                  ve = w;
                  break;
              }
              if (ve >= w) break;
              if (S = K(u.slice(ve, w), !0), W = S.r != null ? parseInt(S.r, 10) : W + 1, M = -1, !(l.sheetRows && l.sheetRows < W)) {
                p.s.r > W - 1 && (p.s.r = W - 1), p.e.r < W - 1 && (p.e.r = W - 1), l && l.cellStyles && (te = {}, oe = !1, S.ht && (oe = !0, te.hpt = parseFloat(S.ht), te.hpx = la(te.hpt)), S.hidden == "1" && (oe = !0, te.hidden = !0), S.outlineLevel != null && (oe = !0, te.level = +S.outlineLevel), oe && (ae[W - 1] = te)), g = u.slice(w).split(e);
                for (var it = 0; it != g.length && g[it].trim().charAt(0) == "<"; ++it) ;
                for (g = g.slice(it), w = 0; w != g.length; ++w)
                  if (u = g[w].trim(), u.length !== 0) {
                    if (C = u.match(t), _ = w, E = 0, R = 0, u = "<c " + (u.slice(0, 1) == "<" ? ">" : "") + u, C != null && C.length === 2) {
                      for (_ = 0, N = C[1], E = 0; E != N.length && !((R = N.charCodeAt(E) - 64) < 1 || R > 26); ++E)
                        _ = 26 * _ + R;
                      --_, M = _;
                    } else ++M;
                    for (E = 0; E != u.length && u.charCodeAt(E) !== 62; ++E) ;
                    if (++E, S = K(u.slice(0, E), !0), S.r || (S.r = ge({ r: W - 1, c: M })), N = u.slice(E), F = { t: "" }, (C = N.match(s)) != null && C[1] !== "" && (F.v = fe(C[1])), l.cellFormula) {
                      if ((C = N.match(i)) != null && C[1] !== "") {
                        if (F.f = fe(ie(C[1])).replace(/\r\n/g, `
`), l.xlfn || (F.f = Bn(F.f)), C[0].indexOf('t="array"') > -1)
                          F.F = (N.match(a) || [])[1], F.F.indexOf(":") > -1 && ee.push([ke(F.F), F.F]);
                        else if (C[0].indexOf('t="shared"') > -1) {
                          I = K(C[0]);
                          var re = fe(ie(C[1]));
                          l.xlfn || (re = Bn(re)), ue[parseInt(I.si, 10)] = [I, re, S.r];
                        }
                      } else (C = N.match(/<f[^>]*\/>/)) && (I = K(C[0]), ue[I.si] && (F.f = Cl(ue[I.si][1], ue[I.si][2], S.r)));
                      var _e = Fe(S.r);
                      for (E = 0; E < ee.length; ++E) _e.r >= ee[E][0].s.r && _e.r <= ee[E][0].e.r && _e.c >= ee[E][0].s.c && _e.c <= ee[E][0].e.c && (F.F = ee[E][1]);
                    }
                    if (S.t == null && F.v === void 0)
                      if (F.f || F.F)
                        F.v = 0, F.t = "n";
                      else if (Pe) F.t = "z";
                      else continue;
                    else F.t = S.t || "n";
                    switch (p.s.c > M && (p.s.c = M), p.e.c < M && (p.e.c = M), F.t) {
                      case "n":
                        if (F.v == "" || F.v == null) {
                          if (!Pe) continue;
                          F.t = "z";
                        } else F.v = parseFloat(F.v);
                        break;
                      case "s":
                        if (typeof F.v > "u") {
                          if (!Pe) continue;
                          F.t = "z";
                        } else
                          L = Cr[parseInt(F.v, 10)], F.v = L.t, F.r = L.r, l.cellHTML && (F.h = L.h);
                        break;
                      case "str":
                        F.t = "s", F.v = F.v != null ? ie(F.v) : "", l.cellHTML && (F.h = Xr(F.v));
                        break;
                      case "inlineStr":
                        C = N.match(n), F.t = "s", C != null && (L = aa(C[1])) ? (F.v = L.t, l.cellHTML && (F.h = L.h)) : F.v = "";
                        break;
                      case "b":
                        F.v = de(F.v);
                        break;
                      case "d":
                        l.cellDates ? F.v = Se(F.v, 1) : (F.v = Oe(Se(F.v, 1)), F.t = "n");
                        break;
                      case "e":
                        (!l || l.cellText !== !1) && (F.w = F.v), F.v = fo[F.v];
                        break;
                    }
                    if (z = G = 0, Y = null, Q && S.s !== void 0 && (Y = m.CellXf[S.s], Y != null && (Y.numFmtId != null && (z = Y.numFmtId), l.cellStyles && Y.fillId != null && (G = Y.fillId))), Ol(F, z, G, l, d, m), l.cellDates && Q && F.t == "n" && dr(ce[z]) && (F.t = "d", F.v = Br(F.v)), S.cm && l.xlmeta) {
                      var ot = (l.xlmeta.Cell || [])[+S.cm - 1];
                      ot && ot.type == "XLDAPR" && (F.D = !0);
                    }
                    if (st) {
                      var lt = Fe(S.r);
                      f[lt.r] || (f[lt.r] = []), f[lt.r][lt.c] = F;
                    } else f[S.r] = F;
                  }
              }
            }
          }
          ae.length > 0 && (f["!rows"] = ae);
        };
      })();
      function lc(e, r, t, n) {
        var a = [], s = [], i = ke(e["!ref"]), o = "", c, f = "", l = [], p = 0, d = 0, m = e["!rows"], w = Array.isArray(e), u = { r: f }, g, C = -1;
        for (d = i.s.c; d <= i.e.c; ++d) l[d] = at(d);
        for (p = i.s.r; p <= i.e.r; ++p) {
          for (s = [], f = qe(p), d = i.s.c; d <= i.e.c; ++d) {
            c = l[d] + f;
            var _ = w ? (e[p] || [])[d] : e[c];
            _ !== void 0 && (o = ic(_, c, e, r)) != null && s.push(o);
          }
          (s.length > 0 || m && m[p]) && (u = { r: f }, m && m[p] && (g = m[p], g.hidden && (u.hidden = 1), C = -1, g.hpx ? C = oa(g.hpx) : g.hpt && (C = g.hpt), C > -1 && (u.ht = C, u.customHeight = 1), g.level && (u.outlineLevel = g.level)), a[a.length] = X("row", s.join(""), u));
        }
        if (m) for (; p < m.length; ++p)
          m && m[p] && (u = { r: p + 1 }, g = m[p], g.hidden && (u.hidden = 1), C = -1, g.hpx ? C = oa(g.hpx) : g.hpt && (C = g.hpt), C > -1 && (u.ht = C, u.customHeight = 1), g.level && (u.outlineLevel = g.level), a[a.length] = X("row", "", u));
        return a.join("");
      }
      function Xn(e, r, t, n) {
        var a = [we, X("worksheet", null, { xmlns: bt[0], "xmlns:r": xe.r })], s = t.SheetNames[e], i = 0, o = "", c = t.Sheets[s];
        c == null && (c = {});
        var f = c["!ref"] || "A1", l = ke(f);
        if (l.e.c > 16383 || l.e.r > 1048575) {
          if (r.WTF) throw new Error("Range " + f + " exceeds format limit A1:XFD1048576");
          l.e.c = Math.min(l.e.c, 16383), l.e.r = Math.min(l.e.c, 1048575), f = ye(l);
        }
        n || (n = {}), c["!comments"] = [];
        var p = [];
        Xl(c, t, e, r, a), a[a.length] = X("dimension", null, { ref: f }), a[a.length] = sc(c, r, e, t), r.sheetFormat && (a[a.length] = X("sheetFormatPr", null, { defaultRowHeight: r.sheetFormat.defaultRowHeight || "16", baseColWidth: r.sheetFormat.baseColWidth || "10", outlineLevelRow: r.sheetFormat.outlineLevelRow || "7" })), c["!cols"] != null && c["!cols"].length > 0 && (a[a.length] = ec(c, c["!cols"])), a[i = a.length] = "<sheetData/>", c["!links"] = [], c["!ref"] != null && (o = lc(c, r), o.length > 0 && (a[a.length] = o)), a.length > i + 1 && (a[a.length] = "</sheetData>", a[i] = a[i].replace("/>", ">")), c["!protect"] && (a[a.length] = ql(c["!protect"])), c["!autofilter"] != null && (a[a.length] = rc(c["!autofilter"], c, t, e)), c["!merges"] != null && c["!merges"].length > 0 && (a[a.length] = Hl(c["!merges"]));
        var d = -1, m, w = -1;
        return c["!links"].length > 0 && (a[a.length] = "<hyperlinks>", c["!links"].forEach(function(u) {
          u[1].Target && (m = { ref: u[0] }, u[1].Target.charAt(0) != "#" && (w = ne(n, -1, he(u[1].Target).replace(/#.*$/, ""), Z.HLINK), m["r:id"] = "rId" + w), (d = u[1].Target.indexOf("#")) > -1 && (m.location = he(u[1].Target.slice(d + 1))), u[1].Tooltip && (m.tooltip = he(u[1].Tooltip)), a[a.length] = X("hyperlink", null, m));
        }), a[a.length] = "</hyperlinks>"), delete c["!links"], c["!margins"] != null && (a[a.length] = Zl(c["!margins"])), (!r || r.ignoreEC || r.ignoreEC == null) && (a[a.length] = Ve("ignoredErrors", X("ignoredError", null, { numberStoredAsText: 1, sqref: f }))), p.length > 0 && (w = ne(n, -1, "../drawings/drawing" + (e + 1) + ".xml", Z.DRAW), a[a.length] = X("drawing", null, { "r:id": "rId" + w }), c["!drawing"] = p), c["!comments"].length > 0 && (w = ne(n, -1, "../drawings/vmlDrawing" + (e + 1) + ".vml", Z.VML), a[a.length] = X("legacyDrawing", null, { "r:id": "rId" + w }), c["!legacy"] = w), a.length > 1 && (a[a.length] = "</worksheet>", a[1] = a[1].replace("/>", ">")), a.join("");
      }
      function cc(e) {
        var r = [], t = e.match(/^<c:numCache>/), n;
        (e.match(/<c:pt idx="(\d*)">(.*?)<\/c:pt>/gm) || []).forEach(function(s) {
          var i = s.match(/<c:pt idx="(\d*?)"><c:v>(.*)<\/c:v><\/c:pt>/);
          i && (r[+i[1]] = t ? +i[2] : i[2]);
        });
        var a = fe((e.match(/<c:formatCode>([\s\S]*?)<\/c:formatCode>/) || ["", "General"])[1]);
        return (e.match(/<c:f>(.*?)<\/c:f>/gm) || []).forEach(function(s) {
          n = s.replace(/<.*?>/g, "");
        }), [r, a, n];
      }
      function fc(e, r, t, n, a, s) {
        var i = s || { "!type": "chart" };
        if (!e) return s;
        var o = 0, c = 0, f = "A", l = { s: { r: 2e6, c: 2e6 }, e: { r: 0, c: 0 } };
        return (e.match(/<c:numCache>[\s\S]*?<\/c:numCache>/gm) || []).forEach(function(p) {
          var d = cc(p);
          l.s.r = l.s.c = 0, l.e.c = o, f = at(o), d[0].forEach(function(m, w) {
            i[f + qe(w)] = { t: "n", v: m, z: d[1] }, c = w;
          }), l.e.r < c && (l.e.r = c), ++o;
        }), o > 0 && (i["!ref"] = ye(l)), i;
      }
      Z.CS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/chartsheet";
      function uc(e, r, t, n, a) {
        if (!e) return e;
        n || (n = { "!id": {} });
        var s = { "!type": "chart", "!drawel": null, "!rel": "" }, i, o = e.match(Vn);
        return o && pa(o[0], s, a, t), (i = e.match(/drawing r:id="(.*?)"/)) && (s["!rel"] = i[1]), n["!id"][s["!rel"]] && (s["!drawel"] = n["!id"][s["!rel"]]), s;
      }
      var ma = [["allowRefreshQuery", !1, "bool"], ["autoCompressPictures", !0, "bool"], ["backupFile", !1, "bool"], ["checkCompatibility", !1, "bool"], ["CodeName", ""], ["date1904", !1, "bool"], ["defaultThemeVersion", 0, "int"], ["filterPrivacy", !1, "bool"], ["hidePivotFieldList", !1, "bool"], ["promptedSolutions", !1, "bool"], ["publishItems", !1, "bool"], ["refreshAllConnections", !1, "bool"], ["saveExternalLinkValues", !0, "bool"], ["showBorderUnselectedTables", !0, "bool"], ["showInkAnnotation", !0, "bool"], ["showObjects", "all"], ["showPivotChartFilter", !1, "bool"], ["updateLinks", "userSet"]], hc = [["activeTab", 0, "int"], ["autoFilterDateGrouping", !0, "bool"], ["firstSheet", 0, "int"], ["minimized", !1, "bool"], ["showHorizontalScroll", !0, "bool"], ["showSheetTabs", !0, "bool"], ["showVerticalScroll", !0, "bool"], ["tabRatio", 600, "int"], ["visibility", "visible"]], pc = [], mc = [["calcCompleted", "true"], ["calcMode", "auto"], ["calcOnSave", "true"], ["concurrentCalc", "true"], ["fullCalcOnLoad", "false"], ["fullPrecision", "true"], ["iterate", "false"], ["iterateCount", "100"], ["iterateDelta", "0.001"], ["refMode", "A1"]];
      function Gn(e, r) {
        for (var t = 0; t != e.length; ++t)
          for (var n = e[t], a = 0; a != r.length; ++a) {
            var s = r[a];
            if (n[s[0]] == null) n[s[0]] = s[1];
            else switch (s[2]) {
              case "bool":
                typeof n[s[0]] == "string" && (n[s[0]] = de(n[s[0]]));
                break;
              case "int":
                typeof n[s[0]] == "string" && (n[s[0]] = parseInt(n[s[0]], 10));
                break;
            }
          }
      }
      function Yn(e, r) {
        for (var t = 0; t != r.length; ++t) {
          var n = r[t];
          if (e[n[0]] == null) e[n[0]] = n[1];
          else switch (n[2]) {
            case "bool":
              typeof e[n[0]] == "string" && (e[n[0]] = de(e[n[0]]));
              break;
            case "int":
              typeof e[n[0]] == "string" && (e[n[0]] = parseInt(e[n[0]], 10));
              break;
          }
        }
      }
      function dc(e) {
        Yn(e.WBProps, ma), Yn(e.CalcPr, mc), Gn(e.WBView, hc), Gn(e.Sheets, pc), Nt.date1904 = de(e.WBProps.date1904);
      }
      var vc = "][*?/\\".split("");
      function qn(e, r) {
        if (e.length > 31)
          throw new Error("Sheet names cannot exceed 31 chars");
        var t = !0;
        return vc.forEach(function(n) {
          if (e.indexOf(n) != -1)
            throw new Error("Sheet name cannot contain : \\ / ? * [ ]");
        }), t;
      }
      function gc(e, r, t) {
        e.forEach(function(n, a) {
          qn(n);
          for (var s = 0; s < a; ++s) if (n == e[s]) throw new Error("Duplicate Sheet Name: " + n);
          if (t) {
            var i = r && r[a] && r[a].CodeName || n;
            if (i.charCodeAt(0) == 95 && i.length > 22) throw new Error("Bad Code Name: Worksheet" + i);
          }
        });
      }
      function Jn(e) {
        if (!e || !e.SheetNames || !e.Sheets) throw new Error("Invalid Workbook");
        if (!e.SheetNames.length) throw new Error("Workbook is empty");
        var r = e.Workbook && e.Workbook.Sheets || [];
        gc(e.SheetNames, r, !!e.vbaraw);
        for (var t = 0; t < e.SheetNames.length; ++t) Ml(e.Sheets[e.SheetNames[t]], e.SheetNames[t], t);
      }
      var bc = /<\w+:workbook/;
      function wc(e, r) {
        if (!e) throw new Error("Could not find file");
        var t = { AppVersion: {}, WBProps: {}, WBView: [], Sheets: [], CalcPr: {}, Names: [], xmlns: "" }, n = !1, a = "xmlns", s = {}, i = 0;
        if (e.replace(Ne, function(c, f) {
          var l = K(c);
          switch (rt(l[0])) {
            case "<?xml":
              break;
            case "<workbook":
              c.match(bc) && (a = "xmlns" + c.match(/<(\w+):/)[1]), t.xmlns = l[a];
              break;
            case "</workbook>":
              break;
            case "<fileVersion":
              delete l[0], t.AppVersion = l;
              break;
            case "<fileVersion/>":
            case "</fileVersion>":
              break;
            case "<fileSharing":
              break;
            case "<fileSharing/>":
              break;
            case "<workbookPr":
            case "<workbookPr/>":
              ma.forEach(function(p) {
                if (l[p[0]] != null)
                  switch (p[2]) {
                    case "bool":
                      t.WBProps[p[0]] = de(l[p[0]]);
                      break;
                    case "int":
                      t.WBProps[p[0]] = parseInt(l[p[0]], 10);
                      break;
                    default:
                      t.WBProps[p[0]] = l[p[0]];
                  }
              }), l.codeName && (t.WBProps.CodeName = ie(l.codeName));
              break;
            case "</workbookPr>":
              break;
            case "<workbookProtection":
              break;
            case "<workbookProtection/>":
              break;
            case "<bookViews":
            case "<bookViews>":
            case "</bookViews>":
              break;
            case "<workbookView":
            case "<workbookView/>":
              delete l[0], t.WBView.push(l);
              break;
            case "</workbookView>":
              break;
            case "<sheets":
            case "<sheets>":
            case "</sheets>":
              break;
            case "<sheet":
              switch (l.state) {
                case "hidden":
                  l.Hidden = 1;
                  break;
                case "veryHidden":
                  l.Hidden = 2;
                  break;
                default:
                  l.Hidden = 0;
              }
              delete l.state, l.name = fe(ie(l.name)), delete l[0], t.Sheets.push(l);
              break;
            case "</sheet>":
              break;
            case "<functionGroups":
            case "<functionGroups/>":
              break;
            case "<functionGroup":
              break;
            case "<externalReferences":
            case "</externalReferences>":
            case "<externalReferences>":
              break;
            case "<externalReference":
              break;
            case "<definedNames/>":
              break;
            case "<definedNames>":
            case "<definedNames":
              n = !0;
              break;
            case "</definedNames>":
              n = !1;
              break;
            case "<definedName":
              s = {}, s.Name = ie(l.name), l.comment && (s.Comment = l.comment), l.localSheetId && (s.Sheet = +l.localSheetId), de(l.hidden || "0") && (s.Hidden = !0), i = f + c.length;
              break;
            case "</definedName>":
              s.Ref = fe(ie(e.slice(i, f))), t.Names.push(s);
              break;
            case "<definedName/>":
              break;
            case "<calcPr":
              delete l[0], t.CalcPr = l;
              break;
            case "<calcPr/>":
              delete l[0], t.CalcPr = l;
              break;
            case "</calcPr>":
              break;
            case "<oleSize":
              break;
            case "<customWorkbookViews>":
            case "</customWorkbookViews>":
            case "<customWorkbookViews":
              break;
            case "<customWorkbookView":
            case "</customWorkbookView>":
              break;
            case "<pivotCaches>":
            case "</pivotCaches>":
            case "<pivotCaches":
              break;
            case "<pivotCache":
              break;
            case "<smartTagPr":
            case "<smartTagPr/>":
              break;
            case "<smartTagTypes":
            case "<smartTagTypes>":
            case "</smartTagTypes>":
              break;
            case "<smartTagType":
              break;
            case "<webPublishing":
            case "<webPublishing/>":
              break;
            case "<fileRecoveryPr":
            case "<fileRecoveryPr/>":
              break;
            case "<webPublishObjects>":
            case "<webPublishObjects":
            case "</webPublishObjects>":
              break;
            case "<webPublishObject":
              break;
            case "<extLst":
            case "<extLst>":
            case "</extLst>":
            case "<extLst/>":
              break;
            case "<ext":
              n = !0;
              break;
            case "</ext>":
              n = !1;
              break;
            case "<ArchID":
              break;
            case "<AlternateContent":
            case "<AlternateContent>":
              n = !0;
              break;
            case "</AlternateContent>":
              n = !1;
              break;
            case "<revisionPtr":
              break;
            default:
              if (!n && r.WTF) throw new Error("unrecognized " + l[0] + " in workbook");
          }
          return c;
        }), bt.indexOf(t.xmlns) === -1) throw new Error("Unknown Namespace: " + t.xmlns);
        return dc(t), t;
      }
      function Kn(e) {
        var r = [we];
        r[r.length] = X("workbook", null, { xmlns: bt[0], "xmlns:r": xe.r });
        var t = e.Workbook && (e.Workbook.Names || []).length > 0, n = { codeName: "ThisWorkbook" };
        e.Workbook && e.Workbook.WBProps && (ma.forEach(function(o) {
          e.Workbook.WBProps[o[0]] != null && e.Workbook.WBProps[o[0]] != o[1] && (n[o[0]] = e.Workbook.WBProps[o[0]]);
        }), e.Workbook.WBProps.CodeName && (n.codeName = e.Workbook.WBProps.CodeName, delete n.CodeName)), r[r.length] = X("workbookPr", null, n);
        var a = e.Workbook && e.Workbook.Sheets || [], s = 0;
        if (a && a[0] && a[0].Hidden) {
          for (r[r.length] = "<bookViews>", s = 0; s != e.SheetNames.length && !(!a[s] || !a[s].Hidden); ++s)
            ;
          s == e.SheetNames.length && (s = 0), r[r.length] = '<workbookView firstSheet="' + s + '" activeTab="' + s + '"/>', r[r.length] = "</bookViews>";
        }
        for (r[r.length] = "<sheets>", s = 0; s != e.SheetNames.length; ++s) {
          var i = { name: he(e.SheetNames[s].slice(0, 31)) };
          if (i.sheetId = "" + (s + 1), i["r:id"] = "rId" + (s + 1), a[s]) switch (a[s].Hidden) {
            case 1:
              i.state = "hidden";
              break;
            case 2:
              i.state = "veryHidden";
              break;
          }
          r[r.length] = X("sheet", null, i);
        }
        return r[r.length] = "</sheets>", t && (r[r.length] = "<definedNames>", e.Workbook && e.Workbook.Names && e.Workbook.Names.forEach(function(o) {
          var c = { name: o.Name };
          o.Comment && (c.comment = o.Comment), o.Sheet != null && (c.localSheetId = "" + o.Sheet), o.Hidden && (c.hidden = "1"), o.Ref && (r[r.length] = X("definedName", he(o.Ref), c));
        }), r[r.length] = "</definedNames>"), r.length > 2 && (r[r.length] = "</workbook>", r[1] = r[1].replace("/>", ">")), r.join("");
      }
      function yc(e, r, t) {
        return r.slice(-4) === ".bin" ? parse_wb_bin(e, t) : wc(e, t);
      }
      function kc(e, r, t, n, a, s, i, o) {
        return r.slice(-4) === ".bin" ? parse_ws_bin(e, n, t, a, s, i, o) : Bl(e, n, t, a, s, i, o);
      }
      function xc(e, r, t, n, a, s, i, o) {
        return r.slice(-4) === ".bin" ? parse_cs_bin(e, n, t, a, s, i, o) : uc(e, n, t, a, s);
      }
      function Sc(e, r, t, n, a, s, i, o) {
        return r.slice(-4) === ".bin" ? yl() : kl();
      }
      function Cc(e, r, t, n, a, s, i, o) {
        return r.slice(-4) === ".bin" ? bl() : wl();
      }
      function _c(e, r, t, n) {
        return r.slice(-4) === ".bin" ? parse_sty_bin(e, t, n) : Zo(e, t, n);
      }
      function Ac(e, r, t) {
        return ll(e, t);
      }
      function Fc(e, r, t) {
        return r.slice(-4) === ".bin" ? parse_sst_bin(e, t) : No(e, t);
      }
      function Tc(e, r, t) {
        return r.slice(-4) === ".bin" ? parse_comments_bin(e, t) : hl(e, t);
      }
      function Ec(e, r, t) {
        return r.slice(-4) === ".bin" ? parse_cc_bin(e, r, t) : parse_cc_xml(e, r, t);
      }
      function Dc(e, r, t, n) {
        return t.slice(-4) === ".bin" ? fl(e, r, t, n) : void 0;
      }
      function Oc(e, r, t) {
        return r.slice(-4) === ".bin" ? parse_xlmeta_bin(e, r, t) : cl(e, r, t);
      }
      function Mc(e, r, t) {
        return (r.slice(-4) === ".bin" ? write_wb_bin : Kn)(e, t);
      }
      function Rc(e, r, t, n, a) {
        return (r.slice(-4) === ".bin" ? write_ws_bin : Xn)(e, t, n, a);
      }
      function Nc(e, r, t) {
        return (r.slice(-4) === ".bin" ? write_sty_bin : Wn)(e, t);
      }
      function Pc(e, r, t) {
        return (r.slice(-4) === ".bin" ? write_sst_bin : Nn)(e, t);
      }
      function Wc(e, r, t) {
        return (r.slice(-4) === ".bin" ? write_comments_bin : zn)(e, t);
      }
      function Ic(e) {
        return (e.slice(-4) === ".bin" ? write_xlmeta_bin : In)();
      }
      function Lc(e, r, t, n) {
        for (var a = e["!merges"] || [], s = [], i = r.s.c; i <= r.e.c; ++i) {
          for (var o = 0, c = 0, f = 0; f < a.length; ++f)
            if (!(a[f].s.r > t || a[f].s.c > i) && !(a[f].e.r < t || a[f].e.c < i)) {
              if (a[f].s.r < t || a[f].s.c < i) {
                o = -1;
                break;
              }
              o = a[f].e.r - a[f].s.r + 1, c = a[f].e.c - a[f].s.c + 1;
              break;
            }
          if (!(o < 0)) {
            var l = ge({ r: t, c: i }), p = n.dense ? (e[t] || [])[i] : e[l], d = p && p.v != null && (p.h || Xr(p.w || (Et(p), p.w) || "")) || "", m = {};
            o > 1 && (m.rowspan = o), c > 1 && (m.colspan = c), n.editable ? d = '<span contenteditable="true">' + d + "</span>" : p && (m["data-t"] = p && p.t || "z", p.v != null && (m["data-v"] = p.v), p.z != null && (m["data-z"] = p.z), p.l && (p.l.Target || "#").charAt(0) != "#" && (d = '<a href="' + p.l.Target + '">' + d + "</a>")), m.id = (n.id || "sjs") + "-" + l, s.push(X("td", d, m));
          }
        }
        var w = "<tr>";
        return w + s.join("") + "</tr>";
      }
      var $c = '<html><head><meta charset="utf-8"/><title>SheetJS Table Export</title></head><body>', zc = "</body></html>";
      function jc(e, r, t) {
        var n = [];
        return n.join("") + "<table" + (t && t.id ? ' id="' + t.id + '"' : "") + ">";
      }
      function Zn(e, r) {
        var t = r || {}, n = t.header != null ? t.header : $c, a = t.footer != null ? t.footer : zc, s = [n], i = nt(e["!ref"]);
        t.dense = Array.isArray(e), s.push(jc(e, i, t));
        for (var o = i.s.r; o <= i.e.r; ++o) s.push(Lc(e, i, o, t));
        return s.push("</table>" + a), s.join("");
      }
      function Qn(e, r, t) {
        var n = t || {}, a = 0, s = 0;
        if (n.origin != null)
          if (typeof n.origin == "number") a = n.origin;
          else {
            var i = typeof n.origin == "string" ? Fe(n.origin) : n.origin;
            a = i.r, s = i.c;
          }
        var o = r.getElementsByTagName("tr"), c = Math.min(n.sheetRows || 1e7, o.length), f = { s: { r: 0, c: 0 }, e: { r: a, c: s } };
        if (e["!ref"]) {
          var l = nt(e["!ref"]);
          f.s.r = Math.min(f.s.r, l.s.r), f.s.c = Math.min(f.s.c, l.s.c), f.e.r = Math.max(f.e.r, l.e.r), f.e.c = Math.max(f.e.c, l.e.c), a == -1 && (f.e.r = a = l.e.r + 1);
        }
        var p = [], d = 0, m = e["!rows"] || (e["!rows"] = []), w = 0, u = 0, g = 0, C = 0, _ = 0, E = 0;
        for (e["!cols"] || (e["!cols"] = []); w < o.length && u < c; ++w) {
          var R = o[w];
          if (ts(R)) {
            if (n.display) continue;
            m[u] = { hidden: !0 };
          }
          var N = R.children;
          for (g = C = 0; g < N.length; ++g) {
            var F = N[g];
            if (!(n.display && ts(F))) {
              var S = F.hasAttribute("data-v") ? F.getAttribute("data-v") : F.hasAttribute("v") ? F.getAttribute("v") : Ri(F.innerHTML), W = F.getAttribute("data-z") || F.getAttribute("z");
              for (d = 0; d < p.length; ++d) {
                var M = p[d];
                M.s.c == C + s && M.s.r < u + a && u + a <= M.e.r && (C = M.e.c + 1 - s, d = -1);
              }
              E = +F.getAttribute("colspan") || 1, ((_ = +F.getAttribute("rowspan") || 1) > 1 || E > 1) && p.push({ s: { r: u + a, c: C + s }, e: { r: u + a + (_ || 1) - 1, c: C + s + (E || 1) - 1 } });
              var L = { t: "s", v: S }, I = F.getAttribute("data-t") || F.getAttribute("t") || "";
              S != null && (S.length == 0 ? L.t = I || "z" : n.raw || S.trim().length == 0 || I == "s" || (S === "TRUE" ? L = { t: "b", v: !0 } : S === "FALSE" ? L = { t: "b", v: !1 } : isNaN(tt(S)) ? isNaN(Xt(S).getDate()) || (L = { t: "d", v: Se(S) }, n.cellDates || (L = { t: "n", v: Oe(L.v) }), L.z = n.dateNF || ce[14]) : L = { t: "n", v: tt(S) })), L.z === void 0 && W != null && (L.z = W);
              var z = "", G = F.getElementsByTagName("A");
              if (G && G.length) for (var Q = 0; Q < G.length && !(G[Q].hasAttribute("href") && (z = G[Q].getAttribute("href"), z.charAt(0) != "#")); ++Q) ;
              z && z.charAt(0) != "#" && (L.l = { Target: z }), n.dense ? (e[u + a] || (e[u + a] = []), e[u + a][C + s] = L) : e[ge({ c: C + s, r: u + a })] = L, f.e.c < C + s && (f.e.c = C + s), C += E;
            }
          }
          ++u;
        }
        return p.length && (e["!merges"] = (e["!merges"] || []).concat(p)), f.e.r = Math.max(f.e.r, u - 1 + a), e["!ref"] = ye(f), u >= c && (e["!fullref"] = ye((f.e.r = o.length - w + u - 1 + a, f))), e;
      }
      function es(e, r) {
        var t = r || {}, n = t.dense ? [] : {};
        return Qn(n, e, r);
      }
      function Uc(e, r) {
        return Dt(es(e, r), r);
      }
      function ts(e) {
        var r = "", t = Bc(e);
        return t && (r = t(e).getPropertyValue("display")), r || (r = e.style && e.style.display), r === "none";
      }
      function Bc(e) {
        return e.ownerDocument.defaultView && typeof e.ownerDocument.defaultView.getComputedStyle == "function" ? e.ownerDocument.defaultView.getComputedStyle : typeof getComputedStyle == "function" ? getComputedStyle : null;
      }
      function Hc(e) {
        var r = e.replace(/[\t\r\n]/g, " ").trim().replace(/ +/g, " ").replace(/<text:s\/>/g, " ").replace(/<text:s text:c="(\d+)"\/>/g, function(n, a) {
          return Array(parseInt(a, 10) + 1).join(" ");
        }).replace(/<text:tab[^>]*\/>/g, "	").replace(/<text:line-break\/>/g, `
`), t = fe(r.replace(/<[^>]*>/g, ""));
        return [t];
      }
      var rs = { day: ["d", "dd"], month: ["m", "mm"], year: ["y", "yy"], hours: ["h", "hh"], minutes: ["m", "mm"], seconds: ["s", "ss"], "am-pm": ["A/P", "AM/PM"], "day-of-week": ["ddd", "dddd"], era: ["e", "ee"], quarter: ["\\Qm", 'm\\"th quarter"'] };
      function Vc(e, r) {
        var t = r || {}, n = un(e), a = [], s, i, o = { name: "" }, c = "", f = 0, l, p, d = {}, m = [], w = t.dense ? [] : {}, u, g, C = { value: "" }, _ = "", E = 0, R = [], N = -1, F = -1, S = { s: { r: 1e6, c: 1e7 }, e: { r: 0, c: 0 } }, W = 0, M = {}, L = [], I = {}, z = 0, G = 0, Q = [], Y = 1, ee = 1, ue = [], st = { Names: [] }, ae = {}, te = ["", ""], oe = [], Pe = {}, je = "", Ue = 0, Ke = !1, Xe = !1, ve = 0;
        for (Yr.lastIndex = 0, n = n.replace(/<!--([\s\S]*?)-->/gm, "").replace(/<!DOCTYPE[^\[]*\[[^\]]*\]>/gm, ""); u = Yr.exec(n); ) switch (u[3] = u[3].replace(/_.*$/, "")) {
          case "table":
          case "工作表":
            u[1] === "/" ? (S.e.c >= S.s.c && S.e.r >= S.s.r ? w["!ref"] = ye(S) : w["!ref"] = "A1:A1", t.sheetRows > 0 && t.sheetRows <= S.e.r && (w["!fullref"] = w["!ref"], S.e.r = t.sheetRows - 1, w["!ref"] = ye(S)), L.length && (w["!merges"] = L), Q.length && (w["!rows"] = Q), l.name = l.名称 || l.name, typeof JSON < "u" && JSON.stringify(l), m.push(l.name), d[l.name] = w, Xe = !1) : u[0].charAt(u[0].length - 2) !== "/" && (l = K(u[0], !1), N = F = -1, S.s.r = S.s.c = 1e7, S.e.r = S.e.c = 0, w = t.dense ? [] : {}, L = [], Q = [], Xe = !0);
            break;
          case "table-row-group":
            u[1] === "/" ? --W : ++W;
            break;
          case "table-row":
          case "行":
            if (u[1] === "/") {
              N += Y, Y = 1;
              break;
            }
            if (p = K(u[0], !1), p.行号 ? N = p.行号 - 1 : N == -1 && (N = 0), Y = +p["number-rows-repeated"] || 1, Y < 10) for (ve = 0; ve < Y; ++ve) W > 0 && (Q[N + ve] = { level: W });
            F = -1;
            break;
          case "covered-table-cell":
            u[1] !== "/" && ++F, t.sheetStubs && (t.dense ? (w[N] || (w[N] = []), w[N][F] = { t: "z" }) : w[ge({ r: N, c: F })] = { t: "z" }), _ = "", R = [];
            break;
          case "table-cell":
          case "数据":
            if (u[0].charAt(u[0].length - 2) === "/")
              ++F, C = K(u[0], !1), ee = parseInt(C["number-columns-repeated"] || "1", 10), g = { t: "z", v: null }, C.formula && t.cellFormula != !1 && (g.f = Hn(fe(C.formula))), (C.数据类型 || C["value-type"]) == "string" && (g.t = "s", g.v = fe(C["string-value"] || ""), t.dense ? (w[N] || (w[N] = []), w[N][F] = g) : w[ge({ r: N, c: F })] = g), F += ee - 1;
            else if (u[1] !== "/") {
              ++F, _ = "", E = 0, R = [], ee = 1;
              var it = Y ? N + Y - 1 : N;
              if (F > S.e.c && (S.e.c = F), F < S.s.c && (S.s.c = F), N < S.s.r && (S.s.r = N), it > S.e.r && (S.e.r = it), C = K(u[0], !1), oe = [], Pe = {}, g = { t: C.数据类型 || C["value-type"], v: null }, t.cellFormula)
                if (C.formula && (C.formula = fe(C.formula)), C["number-matrix-columns-spanned"] && C["number-matrix-rows-spanned"] && (z = parseInt(C["number-matrix-rows-spanned"], 10) || 0, G = parseInt(C["number-matrix-columns-spanned"], 10) || 0, I = { s: { r: N, c: F }, e: { r: N + z - 1, c: F + G - 1 } }, g.F = ye(I), ue.push([I, g.F])), C.formula) g.f = Hn(C.formula);
                else for (ve = 0; ve < ue.length; ++ve) N >= ue[ve][0].s.r && N <= ue[ve][0].e.r && F >= ue[ve][0].s.c && F <= ue[ve][0].e.c && (g.F = ue[ve][1]);
              switch ((C["number-columns-spanned"] || C["number-rows-spanned"]) && (z = parseInt(C["number-rows-spanned"], 10) || 0, G = parseInt(C["number-columns-spanned"], 10) || 0, I = { s: { r: N, c: F }, e: { r: N + z - 1, c: F + G - 1 } }, L.push(I)), C["number-columns-repeated"] && (ee = parseInt(C["number-columns-repeated"], 10)), g.t) {
                case "boolean":
                  g.t = "b", g.v = de(C["boolean-value"]);
                  break;
                case "float":
                  g.t = "n", g.v = parseFloat(C.value);
                  break;
                case "percentage":
                  g.t = "n", g.v = parseFloat(C.value);
                  break;
                case "currency":
                  g.t = "n", g.v = parseFloat(C.value);
                  break;
                case "date":
                  g.t = "d", g.v = Se(C["date-value"]), t.cellDates || (g.t = "n", g.v = Oe(g.v)), g.z = "m/d/yy";
                  break;
                case "time":
                  g.t = "n", g.v = yi(C["time-value"]) / 86400, t.cellDates && (g.t = "d", g.v = Br(g.v)), g.z = "HH:MM:SS";
                  break;
                case "number":
                  g.t = "n", g.v = parseFloat(C.数据数值);
                  break;
                default:
                  if (g.t === "string" || g.t === "text" || !g.t)
                    g.t = "s", C["string-value"] != null && (_ = fe(C["string-value"]), R = []);
                  else throw new Error("Unsupported value type " + g.t);
              }
            } else {
              if (Ke = !1, g.t === "s" && (g.v = _ || "", R.length && (g.R = R), Ke = E == 0), ae.Target && (g.l = ae), oe.length > 0 && (g.c = oe, oe = []), _ && t.cellText !== !1 && (g.w = _), Ke && (g.t = "z", delete g.v), (!Ke || t.sheetStubs) && !(t.sheetRows && t.sheetRows <= N))
                for (var re = 0; re < Y; ++re) {
                  if (ee = parseInt(C["number-columns-repeated"] || "1", 10), t.dense)
                    for (w[N + re] || (w[N + re] = []), w[N + re][F] = re == 0 ? g : Ae(g); --ee > 0; ) w[N + re][F + ee] = Ae(g);
                  else
                    for (w[ge({ r: N + re, c: F })] = g; --ee > 0; ) w[ge({ r: N + re, c: F + ee })] = Ae(g);
                  S.e.c <= F && (S.e.c = F);
                }
              ee = parseInt(C["number-columns-repeated"] || "1", 10), F += ee - 1, ee = 0, g = {}, _ = "", R = [];
            }
            ae = {};
            break;
          case "document":
          case "document-content":
          case "电子表格文档":
          case "spreadsheet":
          case "主体":
          case "scripts":
          case "styles":
          case "font-face-decls":
          case "master-styles":
            if (u[1] === "/") {
              if ((s = a.pop())[0] !== u[3]) throw "Bad state: " + s;
            } else u[0].charAt(u[0].length - 2) !== "/" && a.push([u[3], !0]);
            break;
          case "annotation":
            if (u[1] === "/") {
              if ((s = a.pop())[0] !== u[3]) throw "Bad state: " + s;
              Pe.t = _, R.length && (Pe.R = R), Pe.a = je, oe.push(Pe);
            } else u[0].charAt(u[0].length - 2) !== "/" && a.push([u[3], !1]);
            je = "", Ue = 0, _ = "", E = 0, R = [];
            break;
          case "creator":
            u[1] === "/" ? je = n.slice(Ue, u.index) : Ue = u.index + u[0].length;
            break;
          case "meta":
          case "元数据":
          case "settings":
          case "config-item-set":
          case "config-item-map-indexed":
          case "config-item-map-entry":
          case "config-item-map-named":
          case "shapes":
          case "frame":
          case "text-box":
          case "image":
          case "data-pilot-tables":
          case "list-style":
          case "form":
          case "dde-links":
          case "event-listeners":
          case "chart":
            if (u[1] === "/") {
              if ((s = a.pop())[0] !== u[3]) throw "Bad state: " + s;
            } else u[0].charAt(u[0].length - 2) !== "/" && a.push([u[3], !1]);
            _ = "", E = 0, R = [];
            break;
          case "scientific-number":
            break;
          case "currency-symbol":
            break;
          case "currency-style":
            break;
          case "number-style":
          case "percentage-style":
          case "date-style":
          case "time-style":
            if (u[1] === "/") {
              if (M[o.name] = c, (s = a.pop())[0] !== u[3]) throw "Bad state: " + s;
            } else u[0].charAt(u[0].length - 2) !== "/" && (c = "", o = K(u[0], !1), a.push([u[3], !0]));
            break;
          case "script":
            break;
          case "libraries":
            break;
          case "automatic-styles":
            break;
          case "default-style":
          case "page-layout":
            break;
          case "style":
            break;
          case "map":
            break;
          case "font-face":
            break;
          case "paragraph-properties":
            break;
          case "table-properties":
            break;
          case "table-column-properties":
            break;
          case "table-row-properties":
            break;
          case "table-cell-properties":
            break;
          case "number":
            switch (a[a.length - 1][0]) {
              case "time-style":
              case "date-style":
                i = K(u[0], !1), c += rs[u[3]][i.style === "long" ? 1 : 0];
                break;
            }
            break;
          case "fraction":
            break;
          case "day":
          case "month":
          case "year":
          case "era":
          case "day-of-week":
          case "week-of-year":
          case "quarter":
          case "hours":
          case "minutes":
          case "seconds":
          case "am-pm":
            switch (a[a.length - 1][0]) {
              case "time-style":
              case "date-style":
                i = K(u[0], !1), c += rs[u[3]][i.style === "long" ? 1 : 0];
                break;
            }
            break;
          case "boolean-style":
            break;
          case "boolean":
            break;
          case "text-style":
            break;
          case "text":
            if (u[0].slice(-2) === "/>") break;
            if (u[1] === "/") switch (a[a.length - 1][0]) {
              case "number-style":
              case "date-style":
              case "time-style":
                c += n.slice(f, u.index);
                break;
            }
            else f = u.index + u[0].length;
            break;
          case "named-range":
            i = K(u[0], !1), te = ua(i["cell-range-address"]);
            var _e = { Name: i.name, Ref: te[0] + "!" + te[1] };
            Xe && (_e.Sheet = m.length), st.Names.push(_e);
            break;
          case "text-content":
            break;
          case "text-properties":
            break;
          case "embedded-text":
            break;
          case "body":
          case "电子表格":
            break;
          case "forms":
            break;
          case "table-column":
            break;
          case "table-header-rows":
            break;
          case "table-rows":
            break;
          case "table-column-group":
            break;
          case "table-header-columns":
            break;
          case "table-columns":
            break;
          case "null-date":
            break;
          case "graphic-properties":
            break;
          case "calculation-settings":
            break;
          case "named-expressions":
            break;
          case "label-range":
            break;
          case "label-ranges":
            break;
          case "named-expression":
            break;
          case "sort":
            break;
          case "sort-by":
            break;
          case "sort-groups":
            break;
          case "tab":
            break;
          case "line-break":
            break;
          case "span":
            break;
          case "p":
          case "文本串":
            if (["master-styles"].indexOf(a[a.length - 1][0]) > -1) break;
            if (u[1] === "/" && (!C || !C["string-value"])) {
              var ot = Hc(n.slice(E, u.index));
              _ = (_.length > 0 ? _ + `
` : "") + ot[0];
            } else
              K(u[0], !1), E = u.index + u[0].length;
            break;
          case "s":
            break;
          case "database-range":
            if (u[1] === "/") break;
            try {
              te = ua(K(u[0])["target-range-address"]), d[te[0]]["!autofilter"] = { ref: te[1] };
            } catch {
            }
            break;
          case "date":
            break;
          case "object":
            break;
          case "title":
          case "标题":
            break;
          case "desc":
            break;
          case "binary-data":
            break;
          case "table-source":
            break;
          case "scenario":
            break;
          case "iteration":
            break;
          case "content-validations":
            break;
          case "content-validation":
            break;
          case "help-message":
            break;
          case "error-message":
            break;
          case "database-ranges":
            break;
          case "filter":
            break;
          case "filter-and":
            break;
          case "filter-or":
            break;
          case "filter-condition":
            break;
          case "list-level-style-bullet":
            break;
          case "list-level-style-number":
            break;
          case "list-level-properties":
            break;
          case "sender-firstname":
          case "sender-lastname":
          case "sender-initials":
          case "sender-title":
          case "sender-position":
          case "sender-email":
          case "sender-phone-private":
          case "sender-fax":
          case "sender-company":
          case "sender-phone-work":
          case "sender-street":
          case "sender-city":
          case "sender-postal-code":
          case "sender-country":
          case "sender-state-or-province":
          case "author-name":
          case "author-initials":
          case "chapter":
          case "file-name":
          case "template-name":
          case "sheet-name":
            break;
          case "event-listener":
            break;
          case "initial-creator":
          case "creation-date":
          case "print-date":
          case "generator":
          case "document-statistic":
          case "user-defined":
          case "editing-duration":
          case "editing-cycles":
            break;
          case "config-item":
            break;
          case "page-number":
            break;
          case "page-count":
            break;
          case "time":
            break;
          case "cell-range-source":
            break;
          case "detective":
            break;
          case "operation":
            break;
          case "highlighted-range":
            break;
          case "data-pilot-table":
          case "source-cell-range":
          case "source-service":
          case "data-pilot-field":
          case "data-pilot-level":
          case "data-pilot-subtotals":
          case "data-pilot-subtotal":
          case "data-pilot-members":
          case "data-pilot-member":
          case "data-pilot-display-info":
          case "data-pilot-sort-info":
          case "data-pilot-layout-info":
          case "data-pilot-field-reference":
          case "data-pilot-groups":
          case "data-pilot-group":
          case "data-pilot-group-member":
            break;
          case "rect":
            break;
          case "dde-connection-decls":
          case "dde-connection-decl":
          case "dde-link":
          case "dde-source":
            break;
          case "properties":
            break;
          case "property":
            break;
          case "a":
            if (u[1] !== "/") {
              if (ae = K(u[0], !1), !ae.href) break;
              ae.Target = fe(ae.href), delete ae.href, ae.Target.charAt(0) == "#" && ae.Target.indexOf(".") > -1 ? (te = ua(ae.Target.slice(1)), ae.Target = "#" + te[0] + "!" + te[1]) : ae.Target.match(/^\.\.[\\\/]/) && (ae.Target = ae.Target.slice(3));
            }
            break;
          case "table-protection":
            break;
          case "data-pilot-grand-total":
            break;
          case "office-document-common-attrs":
            break;
          default:
            switch (u[2]) {
              case "dc:":
              case "calcext:":
              case "loext:":
              case "ooo:":
              case "chartooo:":
              case "draw:":
              case "style:":
              case "chart:":
              case "form:":
              case "uof:":
              case "表:":
              case "字:":
                break;
              default:
                if (t.WTF) throw new Error(u);
            }
        }
        var lt = { Sheets: d, SheetNames: m, Workbook: st };
        return t.bookSheets && delete lt.Sheets, lt;
      }
      function as(e, r) {
        r = r || {}, Ie(e, "META-INF/manifest.xml") && po(be(e, "META-INF/manifest.xml"), r);
        var t = Le(e, "content.xml");
        if (!t) throw new Error("Missing content.xml in ODS / UOF file");
        var n = Vc(ie(t), r);
        return Ie(e, "meta.xml") && (n.Props = An(be(e, "meta.xml"))), n;
      }
      var Xc = (function() {
        var e = ["<office:master-styles>", '<style:master-page style:name="mp1" style:page-layout-name="mp1">', "<style:header/>", '<style:header-left style:display="false"/>', "<style:footer/>", '<style:footer-left style:display="false"/>', "</style:master-page>", "</office:master-styles>"].join(""), r = "<office:document-styles " + qt({ "xmlns:office": "urn:oasis:names:tc:opendocument:xmlns:office:1.0", "xmlns:table": "urn:oasis:names:tc:opendocument:xmlns:table:1.0", "xmlns:style": "urn:oasis:names:tc:opendocument:xmlns:style:1.0", "xmlns:text": "urn:oasis:names:tc:opendocument:xmlns:text:1.0", "xmlns:draw": "urn:oasis:names:tc:opendocument:xmlns:drawing:1.0", "xmlns:fo": "urn:oasis:names:tc:opendocument:xmlns:xsl-fo-compatible:1.0", "xmlns:xlink": "http://www.w3.org/1999/xlink", "xmlns:dc": "http://purl.org/dc/elements/1.1/", "xmlns:number": "urn:oasis:names:tc:opendocument:xmlns:datastyle:1.0", "xmlns:svg": "urn:oasis:names:tc:opendocument:xmlns:svg-compatible:1.0", "xmlns:of": "urn:oasis:names:tc:opendocument:xmlns:of:1.2", "office:version": "1.2" }) + ">" + e + "</office:document-styles>";
        return function() {
          return we + r;
        };
      })(), ns = /* @__PURE__ */ (function() {
        var e = function(s) {
          return he(s).replace(/  +/g, function(i) {
            return '<text:s text:c="' + i.length + '"/>';
          }).replace(/\t/g, "<text:tab/>").replace(/\n/g, "</text:p><text:p>").replace(/^ /, "<text:s/>").replace(/ $/, "<text:s/>");
        }, r = `          <table:table-cell />
`, t = `          <table:covered-table-cell/>
`, n = function(s, i, o) {
          var c = [];
          c.push('      <table:table table:name="' + he(i.SheetNames[o]) + `" table:style-name="ta1">
`);
          var f = 0, l = 0, p = nt(s["!ref"] || "A1"), d = s["!merges"] || [], m = 0, w = Array.isArray(s);
          if (s["!cols"])
            for (l = 0; l <= p.e.c; ++l) c.push("        <table:table-column" + (s["!cols"][l] ? ' table:style-name="co' + s["!cols"][l].ods + '"' : "") + `></table:table-column>
`);
          var u = "", g = s["!rows"] || [];
          for (f = 0; f < p.s.r; ++f)
            u = g[f] ? ' table:style-name="ro' + g[f].ods + '"' : "", c.push("        <table:table-row" + u + `></table:table-row>
`);
          for (; f <= p.e.r; ++f) {
            for (u = g[f] ? ' table:style-name="ro' + g[f].ods + '"' : "", c.push("        <table:table-row" + u + `>
`), l = 0; l < p.s.c; ++l) c.push(r);
            for (; l <= p.e.c; ++l) {
              var C = !1, _ = {}, E = "";
              for (m = 0; m != d.length; ++m)
                if (!(d[m].s.c > l) && !(d[m].s.r > f) && !(d[m].e.c < l) && !(d[m].e.r < f)) {
                  (d[m].s.c != l || d[m].s.r != f) && (C = !0), _["table:number-columns-spanned"] = d[m].e.c - d[m].s.c + 1, _["table:number-rows-spanned"] = d[m].e.r - d[m].s.r + 1;
                  break;
                }
              if (C) {
                c.push(t);
                continue;
              }
              var R = ge({ r: f, c: l }), N = w ? (s[f] || [])[l] : s[R];
              if (N && N.f && (_["table:formula"] = he(Al(N.f)), N.F && N.F.slice(0, R.length) == R)) {
                var F = nt(N.F);
                _["table:number-matrix-columns-spanned"] = F.e.c - F.s.c + 1, _["table:number-matrix-rows-spanned"] = F.e.r - F.s.r + 1;
              }
              if (!N) {
                c.push(r);
                continue;
              }
              switch (N.t) {
                case "b":
                  E = N.v ? "TRUE" : "FALSE", _["office:value-type"] = "boolean", _["office:boolean-value"] = N.v ? "true" : "false";
                  break;
                case "n":
                  E = N.w || String(N.v || 0), _["office:value-type"] = "float", _["office:value"] = N.v || 0;
                  break;
                case "s":
                case "str":
                  E = N.v == null ? "" : N.v, _["office:value-type"] = "string";
                  break;
                case "d":
                  E = N.w || Se(N.v).toISOString(), _["office:value-type"] = "date", _["office:date-value"] = Se(N.v).toISOString(), _["table:style-name"] = "ce1";
                  break;
                default:
                  c.push(r);
                  continue;
              }
              var S = e(E);
              if (N.l && N.l.Target) {
                var W = N.l.Target;
                W = W.charAt(0) == "#" ? "#" + Fl(W.slice(1)) : W, W.charAt(0) != "#" && !W.match(/^\w+:/) && (W = "../" + W), S = X("text:a", S, { "xlink:href": W.replace(/&/g, "&amp;") });
              }
              c.push("          " + X("table:table-cell", X("text:p", S, {}), _) + `
`);
            }
            c.push(`        </table:table-row>
`);
          }
          return c.push(`      </table:table>
`), c.join("");
        }, a = function(s, i) {
          s.push(` <office:automatic-styles>
`), s.push(`  <number:date-style style:name="N37" number:automatic-order="true">
`), s.push(`   <number:month number:style="long"/>
`), s.push(`   <number:text>/</number:text>
`), s.push(`   <number:day number:style="long"/>
`), s.push(`   <number:text>/</number:text>
`), s.push(`   <number:year/>
`), s.push(`  </number:date-style>
`);
          var o = 0;
          i.SheetNames.map(function(f) {
            return i.Sheets[f];
          }).forEach(function(f) {
            if (f && f["!cols"]) {
              for (var l = 0; l < f["!cols"].length; ++l) if (f["!cols"][l]) {
                var p = f["!cols"][l];
                if (p.width == null && p.wpx == null && p.wch == null) continue;
                ia(p), p.ods = o;
                var d = f["!cols"][l].wpx + "px";
                s.push('  <style:style style:name="co' + o + `" style:family="table-column">
`), s.push('   <style:table-column-properties fo:break-before="auto" style:column-width="' + d + `"/>
`), s.push(`  </style:style>
`), ++o;
              }
            }
          });
          var c = 0;
          i.SheetNames.map(function(f) {
            return i.Sheets[f];
          }).forEach(function(f) {
            if (f && f["!rows"]) {
              for (var l = 0; l < f["!rows"].length; ++l) if (f["!rows"][l]) {
                f["!rows"][l].ods = c;
                var p = f["!rows"][l].hpx + "px";
                s.push('  <style:style style:name="ro' + c + `" style:family="table-row">
`), s.push('   <style:table-row-properties fo:break-before="auto" style:row-height="' + p + `"/>
`), s.push(`  </style:style>
`), ++c;
              }
            }
          }), s.push(`  <style:style style:name="ta1" style:family="table" style:master-page-name="mp1">
`), s.push(`   <style:table-properties table:display="true" style:writing-mode="lr-tb"/>
`), s.push(`  </style:style>
`), s.push(`  <style:style style:name="ce1" style:family="table-cell" style:parent-style-name="Default" style:data-style-name="N37"/>
`), s.push(` </office:automatic-styles>
`);
        };
        return function(i, o) {
          var c = [we], f = qt({ "xmlns:office": "urn:oasis:names:tc:opendocument:xmlns:office:1.0", "xmlns:table": "urn:oasis:names:tc:opendocument:xmlns:table:1.0", "xmlns:style": "urn:oasis:names:tc:opendocument:xmlns:style:1.0", "xmlns:text": "urn:oasis:names:tc:opendocument:xmlns:text:1.0", "xmlns:draw": "urn:oasis:names:tc:opendocument:xmlns:drawing:1.0", "xmlns:fo": "urn:oasis:names:tc:opendocument:xmlns:xsl-fo-compatible:1.0", "xmlns:xlink": "http://www.w3.org/1999/xlink", "xmlns:dc": "http://purl.org/dc/elements/1.1/", "xmlns:meta": "urn:oasis:names:tc:opendocument:xmlns:meta:1.0", "xmlns:number": "urn:oasis:names:tc:opendocument:xmlns:datastyle:1.0", "xmlns:presentation": "urn:oasis:names:tc:opendocument:xmlns:presentation:1.0", "xmlns:svg": "urn:oasis:names:tc:opendocument:xmlns:svg-compatible:1.0", "xmlns:chart": "urn:oasis:names:tc:opendocument:xmlns:chart:1.0", "xmlns:dr3d": "urn:oasis:names:tc:opendocument:xmlns:dr3d:1.0", "xmlns:math": "http://www.w3.org/1998/Math/MathML", "xmlns:form": "urn:oasis:names:tc:opendocument:xmlns:form:1.0", "xmlns:script": "urn:oasis:names:tc:opendocument:xmlns:script:1.0", "xmlns:ooo": "http://openoffice.org/2004/office", "xmlns:ooow": "http://openoffice.org/2004/writer", "xmlns:oooc": "http://openoffice.org/2004/calc", "xmlns:dom": "http://www.w3.org/2001/xml-events", "xmlns:xforms": "http://www.w3.org/2002/xforms", "xmlns:xsd": "http://www.w3.org/2001/XMLSchema", "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance", "xmlns:sheet": "urn:oasis:names:tc:opendocument:sh33tjs:1.0", "xmlns:rpt": "http://openoffice.org/2005/report", "xmlns:of": "urn:oasis:names:tc:opendocument:xmlns:of:1.2", "xmlns:xhtml": "http://www.w3.org/1999/xhtml", "xmlns:grddl": "http://www.w3.org/2003/g/data-view#", "xmlns:tableooo": "http://openoffice.org/2009/table", "xmlns:drawooo": "http://openoffice.org/2010/draw", "xmlns:calcext": "urn:org:documentfoundation:names:experimental:calc:xmlns:calcext:1.0", "xmlns:loext": "urn:org:documentfoundation:names:experimental:office:xmlns:loext:1.0", "xmlns:field": "urn:openoffice:names:experimental:ooo-ms-interop:xmlns:field:1.0", "xmlns:formx": "urn:openoffice:names:experimental:ooxml-odf-interop:xmlns:form:1.0", "xmlns:css3t": "http://www.w3.org/TR/css3-text/", "office:version": "1.2" }), l = qt({ "xmlns:config": "urn:oasis:names:tc:opendocument:xmlns:config:1.0", "office:mimetype": "application/vnd.oasis.opendocument.spreadsheet" });
          o.bookType == "fods" ? (c.push("<office:document" + f + l + `>
`), c.push(_n().replace(/office:document-meta/g, "office:meta"))) : c.push("<office:document-content" + f + `>
`), a(c, i), c.push(`  <office:body>
`), c.push(`    <office:spreadsheet>
`);
          for (var p = 0; p != i.SheetNames.length; ++p) c.push(n(i.Sheets[i.SheetNames[p]], i, p));
          return c.push(`    </office:spreadsheet>
`), c.push(`  </office:body>
`), o.bookType == "fods" ? c.push("</office:document>") : c.push("</office:document-content>"), c.join("");
        };
      })();
      function ss(e, r) {
        if (r.bookType == "fods") return ns(e, r);
        var t = Vr(), n = "", a = [], s = [];
        return n = "mimetype", J(t, n, "application/vnd.oasis.opendocument.spreadsheet"), n = "content.xml", J(t, n, ns(e, r)), a.push([n, "text/xml"]), s.push([n, "ContentFile"]), n = "styles.xml", J(t, n, Xc(e, r)), a.push([n, "text/xml"]), s.push([n, "StylesFile"]), n = "meta.xml", J(t, n, we + _n()), a.push([n, "text/xml"]), s.push([n, "MetadataFile"]), n = "manifest.rdf", J(t, n, go(s)), a.push([n, "application/rdf+xml"]), n = "META-INF/manifest.xml", J(t, n, mo(a)), t;
      }
      function is(e) {
        return function(t) {
          for (var n = 0; n != e.length; ++n) {
            var a = e[n];
            t[a[0]] === void 0 && (t[a[0]] = a[1]), a[2] === "n" && (t[a[0]] = Number(t[a[0]]));
          }
        };
      }
      function Gc(e) {
        is([["cellNF", !1], ["cellHTML", !0], ["cellFormula", !0], ["cellStyles", !1], ["cellText", !0], ["cellDates", !1], ["sheetStubs", !1], ["sheetRows", 0, "n"], ["bookDeps", !1], ["bookSheets", !1], ["bookProps", !1], ["bookFiles", !1], ["bookVBA", !1], ["password", ""], ["WTF", !1]])(e);
      }
      function os(e) {
        is([["cellDates", !1], ["bookSST", !1], ["bookType", "xlsx"], ["compression", !1], ["WTF", !1]])(e);
      }
      function Yc(e) {
        return Z.WS.indexOf(e) > -1 ? "sheet" : Z.CS && e == Z.CS ? "chart" : e == Z.DS ? "dialog" : e == Z.MS ? "macro" : e && e.length ? e : "sheet";
      }
      function qc(e, r) {
        if (!e) return 0;
        try {
          e = r.map(function(n) {
            return n.id || (n.id = n.strRelID), [n.name, e["!id"][n.id].Target, Yc(e["!id"][n.id].Type)];
          });
        } catch {
          return null;
        }
        return !e || e.length === 0 ? null : e;
      }
      function Jc(e, r, t, n, a, s, i, o, c, f, l, p) {
        try {
          s[n] = Qt(Le(e, t, !0), r);
          var d = be(e, r), m;
          switch (o) {
            case "sheet":
              m = kc(d, r, a, c, s[n], f, l, p);
              break;
            case "chart":
              if (m = xc(d, r, a, c, s[n], f, l, p), !m || !m["!drawel"]) break;
              var w = Gt(m["!drawel"].Target, r), u = Zt(w), g = ul(Le(e, w, !0), Qt(Le(e, u, !0), w)), C = Gt(g, w), _ = Zt(C);
              m = fc(Le(e, C, !0), C, c, Qt(Le(e, _, !0), C), f, m);
              break;
            case "macro":
              m = Sc(d, r, a, c, s[n], f, l, p);
              break;
            case "dialog":
              m = Cc(d, r, a, c, s[n], f, l, p);
              break;
            default:
              throw new Error("Unrecognized sheet type " + o);
          }
          i[n] = m;
          var E = [];
          s && s[n] && De(s[n]).forEach(function(R) {
            var N = "";
            if (s[n][R].Type == Z.CMNT) {
              N = Gt(s[n][R].Target, r);
              var F = Tc(be(e, N, !0), N, c);
              if (!F || !F.length) return;
              $n(m, F, !1);
            }
            s[n][R].Type == Z.TCMNT && (N = Gt(s[n][R].Target, r), E = E.concat(pl(be(e, N, !0), c)));
          }), E && E.length && $n(m, E, !0, c.people || []);
        } catch (R) {
          if (c.WTF) throw R;
        }
      }
      function ze(e) {
        return e.charAt(0) == "/" ? e.slice(1) : e;
      }
      function ls(e, r) {
        if (Ur(), r = r || {}, Gc(r), Ie(e, "META-INF/manifest.xml") || Ie(e, "objectdata.xml")) return as(e, r);
        if (Ie(e, "Index/Document.iwa")) {
          if (typeof Uint8Array > "u") throw new Error("NUMBERS file parsing requires Uint8Array support");
          if (typeof parse_numbers_iwa < "u") {
            if (e.FileIndex) return parse_numbers_iwa(e);
            var t = se.utils.cfb_new();
            return an(e).forEach(function(Q) {
              J(t, Q, Ci(e, Q));
            }), parse_numbers_iwa(t);
          }
          throw new Error("Unsupported NUMBERS file");
        }
        if (!Ie(e, "[Content_Types].xml"))
          throw Ie(e, "index.xml.gz") ? new Error("Unsupported NUMBERS 08 file") : Ie(e, "index.xml") ? new Error("Unsupported NUMBERS 09 file") : new Error("Unsupported ZIP file");
        var n = an(e), a = uo(Le(e, "[Content_Types].xml")), s = !1, i, o;
        if (a.workbooks.length === 0 && (o = "xl/workbook.xml", be(e, o, !0) && a.workbooks.push(o)), a.workbooks.length === 0) {
          if (o = "xl/workbook.bin", !be(e, o, !0)) throw new Error("Could not find workbook");
          a.workbooks.push(o), s = !0;
        }
        a.workbooks[0].slice(-3) == "bin" && (s = !0);
        var c = {}, f = {};
        if (!r.bookSheets && !r.bookProps) {
          if (Cr = [], a.sst) try {
            Cr = Fc(be(e, ze(a.sst)), a.sst, r);
          } catch (Q) {
            if (r.WTF) throw Q;
          }
          r.cellStyles && a.themes.length && (c = Ac(Le(e, a.themes[0].replace(/^\//, ""), !0) || "", a.themes[0], r)), a.style && (f = _c(be(e, ze(a.style)), a.style, c, r));
        }
        a.links.map(function(Q) {
          try {
            var Y = Qt(Le(e, Zt(ze(Q))), Q);
            return Dc(be(e, ze(Q)), Y, Q, r);
          } catch {
          }
        });
        var l = yc(be(e, ze(a.workbooks[0])), a.workbooks[0], r), p = {}, d = "";
        a.coreprops.length && (d = be(e, ze(a.coreprops[0]), !0), d && (p = An(d)), a.extprops.length !== 0 && (d = be(e, ze(a.extprops[0]), !0), d && yo(d, p, r)));
        var m = {};
        (!r.bookSheets || r.bookProps) && a.custprops.length !== 0 && (d = Le(e, ze(a.custprops[0]), !0), d && (m = xo(d, r)));
        var w = {};
        if ((r.bookSheets || r.bookProps) && (l.Sheets ? i = l.Sheets.map(function(Y) {
          return Y.name;
        }) : p.Worksheets && p.SheetNames.length > 0 && (i = p.SheetNames), r.bookProps && (w.Props = p, w.Custprops = m), r.bookSheets && typeof i < "u" && (w.SheetNames = i), r.bookSheets ? w.SheetNames : r.bookProps))
          return w;
        i = {};
        var u = {};
        r.bookDeps && a.calcchain && (u = Ec(be(e, ze(a.calcchain)), a.calcchain, r));
        var g = 0, C = {}, _, E;
        {
          var R = l.Sheets;
          p.Worksheets = R.length, p.SheetNames = [];
          for (var N = 0; N != R.length; ++N)
            p.SheetNames[N] = R[N].name;
        }
        var F = s ? "bin" : "xml", S = a.workbooks[0].lastIndexOf("/"), W = (a.workbooks[0].slice(0, S + 1) + "_rels/" + a.workbooks[0].slice(S + 1) + ".rels").replace(/^\//, "");
        Ie(e, W) || (W = "xl/_rels/workbook." + F + ".rels");
        var M = Qt(Le(e, W, !0), W.replace(/_rels.*/, "s5s"));
        (a.metadata || []).length >= 1 && (r.xlmeta = Oc(be(e, ze(a.metadata[0])), a.metadata[0], r)), (a.people || []).length >= 1 && (r.people = dl(be(e, ze(a.people[0])), r)), M && (M = qc(M, l.Sheets));
        var L = be(e, "xl/worksheets/sheet.xml", !0) ? 1 : 0;
        e: for (g = 0; g != p.Worksheets; ++g) {
          var I = "sheet";
          if (M && M[g] ? (_ = "xl/" + M[g][1].replace(/[\/]?xl\//, ""), Ie(e, _) || (_ = M[g][1]), Ie(e, _) || (_ = W.replace(/_rels\/.*$/, "") + M[g][1]), I = M[g][2]) : (_ = "xl/worksheets/sheet" + (g + 1 - L) + "." + F, _ = _.replace(/sheet0\./, "sheet.")), E = _.replace(/^(.*)(\/)([^\/]*)$/, "$1/_rels/$3.rels"), r && r.sheets != null) switch (typeof r.sheets) {
            case "number":
              if (g != r.sheets) continue e;
              break;
            case "string":
              if (p.SheetNames[g].toLowerCase() != r.sheets.toLowerCase()) continue e;
              break;
            default:
              if (Array.isArray && Array.isArray(r.sheets)) {
                for (var z = !1, G = 0; G != r.sheets.length; ++G)
                  typeof r.sheets[G] == "number" && r.sheets[G] == g && (z = 1), typeof r.sheets[G] == "string" && r.sheets[G].toLowerCase() == p.SheetNames[g].toLowerCase() && (z = 1);
                if (!z) continue e;
              }
          }
          Jc(e, _, E, p.SheetNames[g], g, C, i, I, r, l, c, f);
        }
        return w = { Directory: a, Workbook: l, Props: p, Custprops: m, Deps: u, Sheets: i, SheetNames: p.SheetNames, Strings: Cr, Styles: f, Themes: c, SSF: Ae(ce) }, r && r.bookFiles && (e.files ? (w.keys = n, w.files = e.files) : (w.keys = [], w.files = {}, e.FullPaths.forEach(function(Q, Y) {
          Q = Q.replace(/^Root Entry[\/]/, ""), w.keys.push(Q), w.files[Q] = e.FileIndex[Y];
        }))), r && r.bookVBA && (a.vba.length > 0 ? w.vbaraw = be(e, ze(a.vba[0]), !0) : a.defaults && a.defaults.bin === gl && (w.vbaraw = be(e, "xl/vbaProject.bin", !0))), w;
      }
      function Kc(e, r) {
        var t = r || {}, n = "Workbook", a = se.find(e, n);
        try {
          if (n = "/!DataSpaces/Version", a = se.find(e, n), !a || !a.content) throw new Error("ECMA-376 Encrypted file missing " + n);
          if (parse_DataSpaceVersionInfo(a.content), n = "/!DataSpaces/DataSpaceMap", a = se.find(e, n), !a || !a.content) throw new Error("ECMA-376 Encrypted file missing " + n);
          var s = parse_DataSpaceMap(a.content);
          if (s.length !== 1 || s[0].comps.length !== 1 || s[0].comps[0].t !== 0 || s[0].name !== "StrongEncryptionDataSpace" || s[0].comps[0].v !== "EncryptedPackage") throw new Error("ECMA-376 Encrypted file bad " + n);
          if (n = "/!DataSpaces/DataSpaceInfo/StrongEncryptionDataSpace", a = se.find(e, n), !a || !a.content) throw new Error("ECMA-376 Encrypted file missing " + n);
          var i = parse_DataSpaceDefinition(a.content);
          if (i.length != 1 || i[0] != "StrongEncryptionTransform") throw new Error("ECMA-376 Encrypted file bad " + n);
          if (n = "/!DataSpaces/TransformInfo/StrongEncryptionTransform/!Primary", a = se.find(e, n), !a || !a.content) throw new Error("ECMA-376 Encrypted file missing " + n);
          parse_Primary(a.content);
        } catch {
        }
        if (n = "/EncryptionInfo", a = se.find(e, n), !a || !a.content) throw new Error("ECMA-376 Encrypted file missing " + n);
        var o = parse_EncryptionInfo(a.content);
        if (n = "/EncryptedPackage", a = se.find(e, n), !a || !a.content) throw new Error("ECMA-376 Encrypted file missing " + n);
        if (o[0] == 4 && typeof decrypt_agile < "u") return decrypt_agile(o[1], a.content, t.password || "", t);
        if (o[0] == 2 && typeof decrypt_std76 < "u") return decrypt_std76(o[1], a.content, t.password || "", t);
        throw new Error("File is password-protected");
      }
      function Zc(e, r) {
        return r.bookType == "ods" ? ss(e, r) : r.bookType == "numbers" ? write_numbers_iwa(e, r) : r.bookType == "xlsb" ? Qc(e, r) : cs(e, r);
      }
      function Qc(e, r) {
        Rt = 1024, e && !e.SSF && (e.SSF = Ae(ce)), e && e.SSF && (Ur(), jr(e.SSF), r.revssf = Ja(e.SSF), r.revssf[e.SSF[65535]] = 0, r.ssf = e.SSF), r.rels = {}, r.wbrels = {}, r.Strings = [], r.Strings.Count = 0, r.Strings.Unique = 0, rr ? r.revStrings = /* @__PURE__ */ new Map() : (r.revStrings = {}, r.revStrings.foo = [], delete r.revStrings.foo);
        var t = r.bookType == "xlsb" ? "bin" : "xml", n = jn.indexOf(r.bookType) > -1, a = ea();
        os(r = r || {});
        var s = Vr(), i = "", o = 0;
        if (r.cellXfs = [], ha(r.cellXfs, {}, { revssf: { General: 0 } }), e.Props || (e.Props = {}), i = "docProps/core.xml", J(s, i, Fn(e.Props, r)), a.coreprops.push(i), ne(r.rels, 2, i, Z.CORE_PROPS), i = "docProps/app.xml", !(e.Props && e.Props.SheetNames)) if (!e.Workbook || !e.Workbook.Sheets) e.Props.SheetNames = e.SheetNames;
        else {
          for (var c = [], f = 0; f < e.SheetNames.length; ++f) (e.Workbook.Sheets[f] || {}).Hidden != 2 && c.push(e.SheetNames[f]);
          e.Props.SheetNames = c;
        }
        for (e.Props.Worksheets = e.Props.SheetNames.length, J(s, i, En(e.Props)), a.extprops.push(i), ne(r.rels, 3, i, Z.EXT_PROPS), e.Custprops !== e.Props && De(e.Custprops || {}).length > 0 && (i = "docProps/custom.xml", J(s, i, Dn(e.Custprops)), a.custprops.push(i), ne(r.rels, 4, i, Z.CUST_PROPS)), o = 1; o <= e.SheetNames.length; ++o) {
          var l = { "!id": {} }, p = e.Sheets[e.SheetNames[o - 1]], d = (p || {})["!type"] || "sheet";
          switch (d) {
            case "chart":
            default:
              i = "xl/worksheets/sheet" + o + "." + t, J(s, i, Rc(o - 1, i, r, e, l)), a.sheets.push(i), ne(r.wbrels, -1, "worksheets/sheet" + o + "." + t, Z.WS[0]);
          }
          if (p) {
            var m = p["!comments"], w = !1, u = "";
            m && m.length > 0 && (u = "xl/comments" + o + "." + t, J(s, u, Wc(m, u, r)), a.comments.push(u), ne(l, -1, "../comments" + o + "." + t, Z.CMNT), w = !0), p["!legacy"] && w && J(s, "xl/drawings/vmlDrawing" + o + ".vml", Ln(o, p["!comments"])), delete p["!comments"], delete p["!legacy"];
          }
          l["!id"].rId1 && J(s, Zt(i), Mt(l));
        }
        return r.Strings != null && r.Strings.length > 0 && (i = "xl/sharedStrings." + t, J(s, i, Pc(r.Strings, i, r)), a.strs.push(i), ne(r.wbrels, -1, "sharedStrings." + t, Z.SST)), i = "xl/workbook." + t, J(s, i, Mc(e, i, r)), a.workbooks.push(i), ne(r.rels, 1, i, Z.WB), i = "xl/theme/theme1.xml", J(s, i, ca(e.Themes, r)), a.themes.push(i), ne(r.wbrels, -1, "theme/theme1.xml", Z.THEME), i = "xl/styles." + t, J(s, i, Nc(e, i, r)), a.styles.push(i), ne(r.wbrels, -1, "styles." + t, Z.STY), e.vbaraw && n && (i = "xl/vbaProject.bin", J(s, i, e.vbaraw), a.vba.push(i), ne(r.wbrels, -1, "vbaProject.bin", Z.VBA)), i = "xl/metadata." + t, J(s, i, Ic(i)), a.metadata.push(i), ne(r.wbrels, -1, "metadata." + t, Z.XLMETA), J(s, "[Content_Types].xml", Sn(a, r)), J(s, "_rels/.rels", Mt(r.rels)), J(s, "xl/_rels/workbook." + t + ".rels", Mt(r.wbrels)), delete r.revssf, delete r.ssf, s;
      }
      function cs(e, r) {
        Rt = 1024, e && !e.SSF && (e.SSF = Ae(ce)), e && e.SSF && (Ur(), jr(e.SSF), r.revssf = Ja(e.SSF), r.revssf[e.SSF[65535]] = 0, r.ssf = e.SSF), r.rels = {}, r.wbrels = {}, r.Strings = [], r.Strings.Count = 0, r.Strings.Unique = 0, rr ? r.revStrings = /* @__PURE__ */ new Map() : (r.revStrings = {}, r.revStrings.foo = [], delete r.revStrings.foo);
        var t = "xml", n = jn.indexOf(r.bookType) > -1, a = ea();
        os(r = r || {});
        var s = Vr(), i = "", o = 0;
        if (r.cellXfs = [], ha(r.cellXfs, {}, { revssf: { General: 0 } }), e.Props || (e.Props = {}), i = "docProps/core.xml", J(s, i, Fn(e.Props, r)), a.coreprops.push(i), ne(r.rels, 2, i, Z.CORE_PROPS), i = "docProps/app.xml", !(e.Props && e.Props.SheetNames)) if (!e.Workbook || !e.Workbook.Sheets) e.Props.SheetNames = e.SheetNames;
        else {
          for (var c = [], f = 0; f < e.SheetNames.length; ++f) (e.Workbook.Sheets[f] || {}).Hidden != 2 && c.push(e.SheetNames[f]);
          e.Props.SheetNames = c;
        }
        e.Props.Worksheets = e.Props.SheetNames.length, J(s, i, En(e.Props)), a.extprops.push(i), ne(r.rels, 3, i, Z.EXT_PROPS), e.Custprops !== e.Props && De(e.Custprops || {}).length > 0 && (i = "docProps/custom.xml", J(s, i, Dn(e.Custprops)), a.custprops.push(i), ne(r.rels, 4, i, Z.CUST_PROPS));
        var l = ["SheetJ5"];
        for (r.tcid = 0, o = 1; o <= e.SheetNames.length; ++o) {
          var p = { "!id": {} }, d = e.Sheets[e.SheetNames[o - 1]], m = (d || {})["!type"] || "sheet";
          switch (m) {
            case "chart":
            default:
              i = "xl/worksheets/sheet" + o + "." + t, J(s, i, Xn(o - 1, r, e, p)), a.sheets.push(i), ne(r.wbrels, -1, "worksheets/sheet" + o + "." + t, Z.WS[0]);
          }
          if (d) {
            var w = d["!comments"], u = !1, g = "";
            if (w && w.length > 0) {
              var C = !1;
              w.forEach(function(_) {
                _[1].forEach(function(E) {
                  E.T == !0 && (C = !0);
                });
              }), C && (g = "xl/threadedComments/threadedComment" + o + "." + t, J(s, g, ml(w, l, r)), a.threadedcomments.push(g), ne(p, -1, "../threadedComments/threadedComment" + o + "." + t, Z.TCMNT)), g = "xl/comments" + o + "." + t, J(s, g, zn(w)), a.comments.push(g), ne(p, -1, "../comments" + o + "." + t, Z.CMNT), u = !0;
            }
            d["!legacy"] && u && J(s, "xl/drawings/vmlDrawing" + o + ".vml", Ln(o, d["!comments"])), delete d["!comments"], delete d["!legacy"];
          }
          p["!id"].rId1 && J(s, Zt(i), Mt(p));
        }
        return r.Strings != null && r.Strings.length > 0 && (i = "xl/sharedStrings." + t, J(s, i, Nn(r.Strings, r)), a.strs.push(i), ne(r.wbrels, -1, "sharedStrings." + t, Z.SST)), i = "xl/workbook." + t, J(s, i, Kn(e)), a.workbooks.push(i), ne(r.rels, 1, i, Z.WB), i = "xl/theme/theme1.xml", J(s, i, ca(e.Themes, r)), a.themes.push(i), ne(r.wbrels, -1, "theme/theme1.xml", Z.THEME), i = "xl/styles." + t, J(s, i, Wn(e, r)), a.styles.push(i), ne(r.wbrels, -1, "styles." + t, Z.STY), e.vbaraw && n && (i = "xl/vbaProject.bin", J(s, i, e.vbaraw), a.vba.push(i), ne(r.wbrels, -1, "vbaProject.bin", Z.VBA)), i = "xl/metadata." + t, J(s, i, In()), a.metadata.push(i), ne(r.wbrels, -1, "metadata." + t, Z.XLMETA), l.length > 1 && (i = "xl/persons/person.xml", J(s, i, vl(l)), a.people.push(i), ne(r.wbrels, -1, "persons/person.xml", Z.PEOPLE)), J(s, "[Content_Types].xml", Sn(a, r)), J(s, "_rels/.rels", Mt(r.rels)), J(s, "xl/_rels/workbook." + t + ".rels", Mt(r.wbrels)), delete r.revssf, delete r.ssf, s;
      }
      function da(e, r) {
        var t = "";
        switch ((r || {}).type || "base64") {
          case "buffer":
            return [e[0], e[1], e[2], e[3], e[4], e[5], e[6], e[7]];
          case "base64":
            t = Qe(e.slice(0, 12));
            break;
          case "binary":
            t = e;
            break;
          case "array":
            return [e[0], e[1], e[2], e[3], e[4], e[5], e[6], e[7]];
          default:
            throw new Error("Unrecognized type " + (r && r.type || "undefined"));
        }
        return [t.charCodeAt(0), t.charCodeAt(1), t.charCodeAt(2), t.charCodeAt(3), t.charCodeAt(4), t.charCodeAt(5), t.charCodeAt(6), t.charCodeAt(7)];
      }
      function ef(e, r) {
        return se.find(e, "EncryptedPackage") ? Kc(e, r) : parse_xlscfb(e, r);
      }
      function tf(e, r) {
        var t, n = e, a = r || {};
        return a.type || (a.type = "base64"), t = _i(n, a), ls(t, a);
      }
      function fs(e, r) {
        var t = 0;
        e: for (; t < e.length; ) switch (e.charCodeAt(t)) {
          case 10:
          case 13:
          case 32:
            ++t;
            break;
          case 60:
            return parse_xlml(e.slice(t), r);
          default:
            break e;
        }
        return er.to_workbook(e, r);
      }
      function rf(e, r) {
        var t = "", n = da(e, r);
        switch (r.type) {
          case "base64":
            t = Qe(e);
            break;
          case "binary":
            t = e;
            break;
          case "buffer":
            t = e.toString("binary");
            break;
          case "array":
            t = _t(e);
            break;
          default:
            throw new Error("Unrecognized type " + r.type);
        }
        return n[0] == 239 && n[1] == 187 && n[2] == 191 && (t = ie(t)), r.type = "binary", fs(t, r);
      }
      function af(e, r) {
        var t = e;
        return r.type == "base64" && (t = Qe(t)), t = $cptable.utils.decode(1200, t.slice(2), "str"), r.type = "binary", fs(t, r);
      }
      function nf(e) {
        return e.match(/[^\x00-\x7F]/) ? gt(e) : e;
      }
      function va(e, r, t, n) {
        return n ? (t.type = "string", er.to_workbook(e, t)) : er.to_workbook(r, t);
      }
      function _r(e, r) {
        Nr();
        var t = r || {};
        if (typeof ArrayBuffer < "u" && e instanceof ArrayBuffer) return _r(new Uint8Array(e), (t = Ae(t), t.type = "array", t));
        typeof Uint8Array < "u" && e instanceof Uint8Array && !t.type && (t.type = typeof Deno < "u" ? "buffer" : "array");
        var n = e, a = [0, 0, 0, 0], s = !1;
        if (t.cellStyles && (t.cellNF = !0, t.sheetStubs = !0), Nt = {}, t.dateNF && (Nt.dateNF = t.dateNF), t.type || (t.type = "base64"), t.type == "file" && (t.type = "binary", n = gi(e), typeof Uint8Array < "u" && !Pr && (t.type = "array")), t.type == "string" && (s = !0, t.type = "binary", t.codepage = 65001, n = nf(e)), t.type == "array" && typeof Uint8Array < "u" && e instanceof Uint8Array && typeof ArrayBuffer < "u") {
          var i = new ArrayBuffer(3), o = new Uint8Array(i);
          if (o.foo = "bar", !o.foo)
            return t = Ae(t), t.type = "array", _r(Wr(n), t);
        }
        switch ((a = da(n, t))[0]) {
          case 208:
            if (a[1] === 207 && a[2] === 17 && a[3] === 224 && a[4] === 161 && a[5] === 177 && a[6] === 26 && a[7] === 225) return ef(se.read(n, t), t);
            break;
          case 9:
            if (a[1] <= 8) return parse_xlscfb(n, t);
            break;
          case 60:
            return parse_xlml(n, t);
          case 73:
            if (a[1] === 73 && a[2] === 42 && a[3] === 0) throw new Error("TIFF Image File is not a spreadsheet");
            if (a[1] === 68) return Co(n, t);
            break;
          case 84:
            if (a[1] === 65 && a[2] === 66 && a[3] === 76) return Mn.to_workbook(n, t);
            break;
          case 80:
            return a[1] === 75 && a[2] < 9 && a[3] < 9 ? tf(n, t) : va(e, n, t, s);
          case 239:
            return a[3] === 60 ? parse_xlml(n, t) : va(e, n, t, s);
          case 255:
            if (a[1] === 254)
              return af(n, t);
            if (a[1] === 0 && a[2] === 2 && a[3] === 0) return WK_.to_workbook(n, t);
            break;
          case 0:
            if (a[1] === 0 && (a[2] >= 2 && a[3] === 0 || a[2] === 0 && (a[3] === 8 || a[3] === 9)))
              return WK_.to_workbook(n, t);
            break;
          case 3:
          case 131:
          case 139:
          case 140:
            return ra.to_workbook(n, t);
          case 123:
            if (a[1] === 92 && a[2] === 114 && a[3] === 116) return RTF.to_workbook(n, t);
            break;
          case 10:
          case 13:
          case 32:
            return rf(n, t);
          case 137:
            if (a[1] === 80 && a[2] === 78 && a[3] === 71) throw new Error("PNG Image File is not a spreadsheet");
            break;
        }
        return So.indexOf(a[0]) > -1 && a[2] <= 12 && a[3] <= 31 ? ra.to_workbook(n, t) : va(e, n, t, s);
      }
      function us(e, r) {
        var t = r || {};
        return t.type = "file", _r(e, t);
      }
      function hs(e, r) {
        switch (r.type) {
          case "base64":
          case "binary":
            break;
          case "buffer":
          case "array":
            r.type = "";
            break;
          case "file":
            return Vt(r.file, se.write(e, { type: "" }));
          case "string":
            throw new Error("'string' output type invalid for '" + r.bookType + "' files");
          default:
            throw new Error("Unrecognized type " + r.type);
        }
        return se.write(e, r);
      }
      function sf(e, r) {
        var t = Ae(r || {}), n = Zc(e, t);
        return ps(n, t);
      }
      function of(e, r) {
        var t = Ae(r || {}), n = cs(e, t);
        return ps(n, t);
      }
      function ps(e, r) {
        var t = {}, n = typeof Uint8Array < "u" ? "array" : "string";
        if (r.compression && (t.compression = "DEFLATE"), r.password) t.type = n;
        else switch (r.type) {
          case "base64":
            t.type = "base64";
            break;
          case "binary":
            t.type = "string";
            break;
          case "string":
            throw new Error("'string' output type invalid for '" + r.bookType + "' files");
          case "buffer":
          case "file":
            t.type = n;
            break;
          default:
            throw new Error("Unrecognized type " + r.type);
        }
        var a = e.FullPaths ? se.write(e, { fileType: "zip", type: { nodebuffer: "buffer", string: "binary" }[t.type] || t.type, compression: !!r.compression }) : e.generate(t);
        if (typeof Deno < "u" && typeof a == "string") {
          if (r.type == "binary" || r.type == "base64") return a;
          a = new Uint8Array(zt(a));
        }
        return r.password && typeof encrypt_agile < "u" ? hs(encrypt_agile(a, r.password), r) : r.type === "file" ? Vt(r.file, a) : r.type == "string" ? ie(a) : a;
      }
      function lf(e, r) {
        var t = r || {}, n = write_xlscfb(e, t);
        return hs(n, t);
      }
      function Je(e, r, t) {
        t || (t = "");
        var n = t + e;
        switch (r.type) {
          case "base64":
            return Lt(gt(n));
          case "binary":
            return gt(n);
          case "string":
            return e;
          case "file":
            return Vt(r.file, n, "utf8");
          case "buffer":
            return typeof TextEncoder < "u" ? new TextEncoder().encode(n) : Je(n, { type: "binary" }).split("").map(function(a) {
              return a.charCodeAt(0);
            });
        }
        throw new Error("Unrecognized type " + r.type);
      }
      function cf(e, r) {
        switch (r.type) {
          case "base64":
            return Lt(e);
          case "binary":
            return e;
          case "string":
            return e;
          case "file":
            return Vt(r.file, e, "binary");
          case "buffer":
            return e.split("").map(function(t) {
              return t.charCodeAt(0);
            });
        }
        throw new Error("Unrecognized type " + r.type);
      }
      function Ar(e, r) {
        switch (r.type) {
          case "string":
          case "base64":
          case "binary":
            for (var t = "", n = 0; n < e.length; ++n) t += String.fromCharCode(e[n]);
            return r.type == "base64" ? Lt(t) : r.type == "string" ? ie(t) : t;
          case "file":
            return Vt(r.file, e);
          case "buffer":
            return e;
          default:
            throw new Error("Unrecognized type " + r.type);
        }
      }
      function ga(e, r) {
        Nr(), Jn(e);
        var t = Ae(r || {});
        if (t.cellStyles && (t.cellNF = !0, t.sheetStubs = !0), t.type == "array") {
          t.type = "binary";
          var n = ga(e, t);
          return t.type = "array", zt(n);
        }
        return of(e, t);
      }
      function Fr(e, r) {
        Nr(), Jn(e);
        var t = Ae(r || {});
        if (t.cellStyles && (t.cellNF = !0, t.sheetStubs = !0), t.type == "array") {
          t.type = "binary";
          var n = Fr(e, t);
          return t.type = "array", zt(n);
        }
        var a = 0;
        if (t.sheet && (typeof t.sheet == "number" ? a = t.sheet : a = e.SheetNames.indexOf(t.sheet), !e.SheetNames[a]))
          throw new Error("Sheet not found: " + t.sheet + " : " + typeof t.sheet);
        switch (t.bookType || "xlsb") {
          case "xml":
          case "xlml":
            return Je(write_xlml(e, t), t);
          case "slk":
          case "sylk":
            return Je(On.from_sheet(e.Sheets[e.SheetNames[a]], t), t);
          case "htm":
          case "html":
            return Je(Zn(e.Sheets[e.SheetNames[a]], t), t);
          case "txt":
            return cf(vs(e.Sheets[e.SheetNames[a]], t), t);
          case "csv":
            return Je(ya(e.Sheets[e.SheetNames[a]], t), t, "\uFEFF");
          case "dif":
            return Je(Mn.from_sheet(e.Sheets[e.SheetNames[a]], t), t);
          case "dbf":
            return Ar(ra.from_sheet(e.Sheets[e.SheetNames[a]], t), t);
          case "prn":
            return Je(er.from_sheet(e.Sheets[e.SheetNames[a]], t), t);
          case "rtf":
            return Je(RTF.from_sheet(e.Sheets[e.SheetNames[a]], t), t);
          case "eth":
            return Je(Rn.from_sheet(e.Sheets[e.SheetNames[a]], t), t);
          case "fods":
            return Je(ss(e, t), t);
          case "wk1":
            return Ar(WK_.sheet_to_wk1(e.Sheets[e.SheetNames[a]], t), t);
          case "wk3":
            return Ar(WK_.book_to_wk3(e, t), t);
          case "biff2":
            t.biff || (t.biff = 2);
          case "biff3":
            t.biff || (t.biff = 3);
          case "biff4":
            return t.biff || (t.biff = 4), Ar(write_biff_buf(e, t), t);
          case "biff5":
            t.biff || (t.biff = 5);
          case "biff8":
          case "xla":
          case "xls":
            return t.biff || (t.biff = 8), lf(e, t);
          case "xlsx":
          case "xlsm":
          case "xlam":
          case "xlsb":
          case "numbers":
          case "ods":
            return sf(e, t);
          default:
            throw new Error("Unrecognized bookType |" + t.bookType + "|");
        }
      }
      function ba(e) {
        if (!e.bookType) {
          var r = { xls: "biff8", htm: "html", slk: "sylk", socialcalc: "eth", Sh33tJS: "WTF" }, t = e.file.slice(e.file.lastIndexOf(".")).toLowerCase();
          t.match(/^\.[a-z]+$/) && (e.bookType = t.slice(1)), e.bookType = r[e.bookType] || e.bookType;
        }
      }
      function ms(e, r, t) {
        var n = t || {};
        return n.type = "file", n.file = r, ba(n), Fr(e, n);
      }
      function ff(e, r, t) {
        var n = t || {};
        return n.type = "file", n.file = r, ba(n), ga(e, n);
      }
      function uf(e, r, t, n) {
        var a = t || {};
        a.type = "file", a.file = e, ba(a), a.type = "buffer";
        var s = n;
        return s instanceof Function || (s = t), mt.writeFile(e, Fr(r, a), s);
      }
      function hf(e, r, t, n, a, s, i, o) {
        var c = qe(t), f = o.defval, l = o.raw || !Object.prototype.hasOwnProperty.call(o, "raw"), p = !0, d = a === 1 ? [] : {};
        if (a !== 1)
          if (Object.defineProperty) try {
            Object.defineProperty(d, "__rowNum__", { value: t, enumerable: !1 });
          } catch {
            d.__rowNum__ = t;
          }
          else d.__rowNum__ = t;
        if (!i || e[t]) for (var m = r.s.c; m <= r.e.c; ++m) {
          var w = i ? e[t][m] : e[n[m] + c];
          if (w === void 0 || w.t === void 0) {
            if (f === void 0) continue;
            s[m] != null && (d[s[m]] = f);
            continue;
          }
          var u = w.v;
          switch (w.t) {
            case "z":
              if (u == null) break;
              continue;
            case "e":
              u = u == 0 ? null : void 0;
              break;
            case "s":
            case "d":
            case "b":
            case "n":
              break;
            default:
              throw new Error("unrecognized type " + w.t);
          }
          if (s[m] != null) {
            if (u == null)
              if (w.t == "e" && u === null) d[s[m]] = null;
              else if (f !== void 0) d[s[m]] = f;
              else if (l && u === null) d[s[m]] = null;
              else continue;
            else
              d[s[m]] = l && (w.t !== "n" || w.t === "n" && o.rawNumbers !== !1) ? u : Et(w, u, o);
            u != null && (p = !1);
          }
        }
        return { row: d, isempty: p };
      }
      function wa(e, r) {
        if (e == null || e["!ref"] == null) return [];
        var t = { t: "n", v: 0 }, n = 0, a = 1, s = [], i = 0, o = "", c = { s: { r: 0, c: 0 }, e: { r: 0, c: 0 } }, f = r || {}, l = f.range != null ? f.range : e["!ref"];
        switch (f.header === 1 ? n = 1 : f.header === "A" ? n = 2 : Array.isArray(f.header) ? n = 3 : f.header == null && (n = 0), typeof l) {
          case "string":
            c = ke(l);
            break;
          case "number":
            c = ke(e["!ref"]), c.s.r = l;
            break;
          default:
            c = l;
        }
        n > 0 && (a = 0);
        var p = qe(c.s.r), d = [], m = [], w = 0, u = 0, g = Array.isArray(e), C = c.s.r, _ = 0, E = {};
        g && !e[C] && (e[C] = []);
        var R = f.skipHidden && e["!cols"] || [], N = f.skipHidden && e["!rows"] || [];
        for (_ = c.s.c; _ <= c.e.c; ++_)
          if (!(R[_] || {}).hidden)
            switch (d[_] = at(_), t = g ? e[C][_] : e[d[_] + p], n) {
              case 1:
                s[_] = _ - c.s.c;
                break;
              case 2:
                s[_] = d[_];
                break;
              case 3:
                s[_] = f.header[_ - c.s.c];
                break;
              default:
                if (t == null && (t = { w: "__EMPTY", t: "s" }), o = i = Et(t, null, f), u = E[i] || 0, !u) E[i] = 1;
                else {
                  do
                    o = i + "_" + u++;
                  while (E[o]);
                  E[i] = u, E[o] = 1;
                }
                s[_] = o;
            }
        for (C = c.s.r + a; C <= c.e.r; ++C)
          if (!(N[C] || {}).hidden) {
            var F = hf(e, c, C, d, n, s, g, f);
            (F.isempty === !1 || (n === 1 ? f.blankrows !== !1 : f.blankrows)) && (m[w++] = F.row);
          }
        return m.length = w, m;
      }
      var ds = /"/g;
      function pf(e, r, t, n, a, s, i, o) {
        for (var c = !0, f = [], l = "", p = qe(t), d = r.s.c; d <= r.e.c; ++d)
          if (n[d]) {
            var m = o.dense ? (e[t] || [])[d] : e[n[d] + p];
            if (m == null) l = "";
            else if (m.v != null) {
              c = !1, l = "" + (o.rawNumbers && m.t == "n" ? m.v : Et(m, null, o));
              for (var w = 0, u = 0; w !== l.length; ++w) if ((u = l.charCodeAt(w)) === a || u === s || u === 34 || o.forceQuotes) {
                l = '"' + l.replace(ds, '""') + '"';
                break;
              }
              l == "ID" && (l = '"ID"');
            } else m.f != null && !m.F ? (c = !1, l = "=" + m.f, l.indexOf(",") >= 0 && (l = '"' + l.replace(ds, '""') + '"')) : l = "";
            f.push(l);
          }
        return o.blankrows === !1 && c ? null : f.join(i);
      }
      function ya(e, r) {
        var t = [], n = r ?? {};
        if (e == null || e["!ref"] == null) return "";
        var a = ke(e["!ref"]), s = n.FS !== void 0 ? n.FS : ",", i = s.charCodeAt(0), o = n.RS !== void 0 ? n.RS : `
`, c = o.charCodeAt(0), f = new RegExp((s == "|" ? "\\|" : s) + "+$"), l = "", p = [];
        n.dense = Array.isArray(e);
        for (var d = n.skipHidden && e["!cols"] || [], m = n.skipHidden && e["!rows"] || [], w = a.s.c; w <= a.e.c; ++w) (d[w] || {}).hidden || (p[w] = at(w));
        for (var u = 0, g = a.s.r; g <= a.e.r; ++g)
          (m[g] || {}).hidden || (l = pf(e, a, g, p, i, c, s, n), l != null && (n.strip && (l = l.replace(f, "")), (l || n.blankrows !== !1) && t.push((u++ ? o : "") + l)));
        return delete n.dense, t.join("");
      }
      function vs(e, r) {
        r || (r = {}), r.FS = "	", r.RS = `
`;
        var t = ya(e, r);
        if (typeof $cptable > "u" || r.type == "string") return t;
        var n = $cptable.utils.encode(1200, t, "str");
        return "ÿþ" + n;
      }
      function mf(e) {
        var r = "", t, n = "";
        if (e == null || e["!ref"] == null) return [];
        var a = ke(e["!ref"]), s = "", i = [], o, c = [], f = Array.isArray(e);
        for (o = a.s.c; o <= a.e.c; ++o) i[o] = at(o);
        for (var l = a.s.r; l <= a.e.r; ++l)
          for (s = qe(l), o = a.s.c; o <= a.e.c; ++o)
            if (r = i[o] + s, t = f ? (e[l] || [])[o] : e[r], n = "", t !== void 0) {
              if (t.F != null) {
                if (r = t.F, !t.f) continue;
                n = t.f, r.indexOf(":") == -1 && (r = r + ":" + r);
              }
              if (t.f != null) n = t.f;
              else {
                if (t.t == "z") continue;
                if (t.t == "n" && t.v != null) n = "" + t.v;
                else if (t.t == "b") n = t.v ? "TRUE" : "FALSE";
                else if (t.w !== void 0) n = "'" + t.w;
                else {
                  if (t.v === void 0) continue;
                  t.t == "s" ? n = "'" + t.v : n = "" + t.v;
                }
              }
              c[c.length] = r + "=" + n;
            }
        return c;
      }
      function gs(e, r, t) {
        var n = t || {}, a = +!n.skipHeader, s = e || {}, i = 0, o = 0;
        if (s && n.origin != null)
          if (typeof n.origin == "number") i = n.origin;
          else {
            var c = typeof n.origin == "string" ? Fe(n.origin) : n.origin;
            i = c.r, o = c.c;
          }
        var f, l = { s: { c: 0, r: 0 }, e: { c: o, r: i + r.length - 1 + a } };
        if (s["!ref"]) {
          var p = ke(s["!ref"]);
          l.e.c = Math.max(l.e.c, p.e.c), l.e.r = Math.max(l.e.r, p.e.r), i == -1 && (i = p.e.r + 1, l.e.r = i + r.length - 1 + a);
        } else
          i == -1 && (i = 0, l.e.r = r.length - 1 + a);
        var d = n.header || [], m = 0;
        r.forEach(function(u, g) {
          De(u).forEach(function(C) {
            (m = d.indexOf(C)) == -1 && (d[m = d.length] = C);
            var _ = u[C], E = "z", R = "", N = ge({ c: o + m, r: i + g + a });
            f = ar(s, N), _ && typeof _ == "object" && !(_ instanceof Date) ? s[N] = _ : (typeof _ == "number" ? E = "n" : typeof _ == "boolean" ? E = "b" : typeof _ == "string" ? E = "s" : _ instanceof Date ? (E = "d", n.cellDates || (E = "n", _ = Oe(_)), R = n.dateNF || ce[14]) : _ === null && n.nullError && (E = "e", _ = 0), f ? (f.t = E, f.v = _, delete f.w, delete f.R, R && (f.z = R)) : s[N] = f = { t: E, v: _ }, R && (f.z = R));
          });
        }), l.e.c = Math.max(l.e.c, o + d.length - 1);
        var w = qe(i);
        if (a) for (m = 0; m < d.length; ++m) s[at(m + o) + w] = { t: "s", v: d[m] };
        return s["!ref"] = ye(l), s;
      }
      function df(e, r) {
        return gs(null, e, r);
      }
      function ar(e, r, t) {
        if (typeof r == "string") {
          if (Array.isArray(e)) {
            var n = Fe(r);
            return e[n.r] || (e[n.r] = []), e[n.r][n.c] || (e[n.r][n.c] = { t: "z" });
          }
          return e[r] || (e[r] = { t: "z" });
        }
        return typeof r != "number" ? ar(e, ge(r)) : ar(e, ge({ r, c: t || 0 }));
      }
      function vf(e, r) {
        if (typeof r == "number") {
          if (r >= 0 && e.SheetNames.length > r) return r;
          throw new Error("Cannot find sheet # " + r);
        } else if (typeof r == "string") {
          var t = e.SheetNames.indexOf(r);
          if (t > -1) return t;
          throw new Error("Cannot find sheet name |" + r + "|");
        } else throw new Error("Cannot find sheet |" + r + "|");
      }
      function gf() {
        return { SheetNames: [], Sheets: {} };
      }
      function bf(e, r, t, n) {
        var a = 1;
        if (!t) for (; a <= 65535 && e.SheetNames.indexOf(t = "Sheet" + a) != -1; ++a, t = void 0) ;
        if (!t || e.SheetNames.length >= 65535) throw new Error("Too many worksheets");
        if (n && e.SheetNames.indexOf(t) >= 0) {
          var s = t.match(/(^.*?)(\d+)$/);
          a = s && +s[2] || 0;
          var i = s && s[1] || t;
          for (++a; a <= 65535 && e.SheetNames.indexOf(t = i + a) != -1; ++a) ;
        }
        if (qn(t), e.SheetNames.indexOf(t) >= 0) throw new Error("Worksheet with name |" + t + "| already exists!");
        return e.SheetNames.push(t), e.Sheets[t] = r, t;
      }
      function wf(e, r, t) {
        e.Workbook || (e.Workbook = {}), e.Workbook.Sheets || (e.Workbook.Sheets = []);
        var n = vf(e, r);
        switch (e.Workbook.Sheets[n] || (e.Workbook.Sheets[n] = {}), t) {
          case 0:
          case 1:
          case 2:
            break;
          default:
            throw new Error("Bad sheet visibility setting " + t);
        }
        e.Workbook.Sheets[n].Hidden = t;
      }
      function yf(e, r) {
        return e.z = r, e;
      }
      function bs(e, r, t) {
        return r ? (e.l = { Target: r }, t && (e.l.Tooltip = t)) : delete e.l, e;
      }
      function kf(e, r, t) {
        return bs(e, "#" + r, t);
      }
      function xf(e, r, t) {
        e.c || (e.c = []), e.c.push({ t: r, a: t || "SheetJS" });
      }
      function Sf(e, r, t, n) {
        for (var a = typeof r != "string" ? r : ke(r), s = typeof r == "string" ? r : ye(r), i = a.s.r; i <= a.e.r; ++i) for (var o = a.s.c; o <= a.e.c; ++o) {
          var c = ar(e, i, o);
          c.t = "n", c.F = s, delete c.v, i == a.s.r && o == a.s.c && (c.f = t, n && (c.D = !0));
        }
        return e;
      }
      var Cf = { encode_col: at, encode_row: qe, encode_cell: ge, encode_range: ye, decode_col: Jr, decode_row: qr, split_cell: oo, decode_cell: Fe, decode_range: nt, format_cell: Et, sheet_add_aoa: xn, sheet_add_json: gs, sheet_add_dom: Qn, aoa_to_sheet: Ot, json_to_sheet: df, table_to_sheet: es, table_to_book: Uc, sheet_to_csv: ya, sheet_to_txt: vs, sheet_to_json: wa, sheet_to_html: Zn, sheet_to_formulae: mf, sheet_to_row_object_array: wa, sheet_get_cell: ar, book_new: gf, book_append_sheet: bf, book_set_sheet_visibility: wf, cell_set_number_format: yf, cell_set_hyperlink: bs, cell_set_internal_link: kf, cell_add_comment: xf, sheet_set_array_formula: Sf, consts: { SHEET_VISIBLE: 0, SHEET_HIDDEN: 1, SHEET_VERY_HIDDEN: 2 } };
      if (typeof parse_xlscfb < "u" && (le.parse_xlscfb = parse_xlscfb), le.parse_zip = ls, le.read = _r, le.readFile = us, le.readFileSync = us, le.write = Fr, le.writeFile = ms, le.writeFileSync = ms, le.writeFileAsync = uf, le.utils = Cf, le.writeXLSX = ga, le.writeFileXLSX = ff, le.SSF = Ga, typeof __stream < "u" && (le.stream = __stream), typeof se < "u" && (le.CFB = se), typeof Ns < "u") {
        var ws = void 0;
        (ws || {}).Readable && set_readable(ws.Readable);
      }
    }
    if (dt(or), typeof window < "u" && !window.XLSX) try {
      window.XLSX = Wt;
    } catch {
    }
  })(Or, Or.exports)), Or.exports;
}
var Ws = /* @__PURE__ */ Vf();
const Xf = /* @__PURE__ */ Bf(Ws), qf = /* @__PURE__ */ Hf({
  __proto__: null,
  default: Xf
}, [Ws]);
export {
  qf as x
};
