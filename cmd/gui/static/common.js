// kividb-operator GUI: what every page shares. Vanilla JS, no build step;
// the page scripts (dashboard.js, detail.js, create.js) use window.KV.
(function () {
  "use strict";

  var KV = (window.KV = {});
  KV.REFRESH_MS = 10000;
  KV.session = { writable: false, user: "", watchNamespace: "", defaultImage: "" };

  // ---------- DOM ----------

  KV.qs = function (id) {
    return document.getElementById(id);
  };

  // el("div", {className: "x", onclick: fn}, child, "text", [more, children], ...)
  KV.el = function (tag, props) {
    var node = document.createElement(tag);
    if (props) {
      Object.keys(props).forEach(function (k) {
        var v = props[k];
        if (v === undefined || v === null) return;
        if (k === "className") node.className = v;
        else if (k === "text") node.textContent = v;
        else if (k.slice(0, 2) === "on") node.addEventListener(k.slice(2), v);
        else if (k === "dataset") Object.assign(node.dataset, v);
        else node.setAttribute(k, v);
      });
    }
    var add = function (c) {
      if (c === null || c === undefined || c === false) return;
      if (Array.isArray(c)) c.forEach(add);
      else node.appendChild(typeof c === "string" || typeof c === "number" ? document.createTextNode(String(c)) : c);
    };
    for (var i = 2; i < arguments.length; i++) add(arguments[i]);
    return node;
  };

  KV.clear = function (node) {
    while (node && node.firstChild) node.removeChild(node.firstChild);
    return node;
  };

  // ---------- formatting ----------

  KV.dash = function (v) {
    return v === null || v === undefined || v === "" ? "–" : String(v);
  };

  KV.fmtBytes = function (n) {
    n = Number(n);
    if (!isFinite(n) || n < 0) return "–";
    if (n < 1024) return Math.round(n) + " B";
    if (n < 1048576) return (n / 1024).toFixed(1) + " KiB";
    if (n < 1073741824) return (n / 1048576).toFixed(1) + " MiB";
    return (n / 1073741824).toFixed(2) + " GiB";
  };

  KV.fmtNum = function (n) {
    n = Number(n);
    if (!isFinite(n)) return "–";
    var a = Math.abs(n);
    if (a >= 1e9) return (n / 1e9).toFixed(a >= 1e10 ? 0 : 1) + "G";
    if (a >= 1e6) return (n / 1e6).toFixed(a >= 1e7 ? 0 : 1) + "M";
    if (a >= 1e4) return (n / 1e3).toFixed(a >= 1e5 ? 0 : 1) + "k";
    if (a >= 100 || n === Math.round(n)) return String(Math.round(n));
    return n.toFixed(1);
  };

  KV.fmtTime = function (iso) {
    if (!iso) return "–";
    var d = new Date(iso);
    return isNaN(d.getTime()) ? "–" : d.toLocaleString();
  };

  KV.fmtAgo = function (iso) {
    if (!iso) return "–";
    var t = new Date(iso).getTime();
    if (isNaN(t)) return "–";
    var s = Math.max(0, (Date.now() - t) / 1000);
    if (s < 60) return Math.round(s) + "s ago";
    if (s < 3600) return Math.round(s / 60) + "m ago";
    if (s < 86400) return Math.round(s / 3600) + "h ago";
    return Math.round(s / 86400) + "d ago";
  };

  KV.fmtClock = function (ms, withDate) {
    var d = new Date(ms);
    var hm = d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
    return withDate ? d.toLocaleDateString([], { month: "short", day: "numeric" }) + " " + hm : hm;
  };

  // Phases the operator reports, grouped the way the dashboard counts them.
  KV.phaseGroup = function (phase) {
    switch (phase) {
      case "Running":
        return "healthy";
      case "Error":
        return "error";
      default:
        return "attention";
    }
  };

  KV.phaseClass = function (phase) {
    var g = KV.phaseGroup(phase);
    if (g === "healthy") return "phase-green";
    if (g === "error") return "phase-red";
    return phase ? "phase-yellow" : "";
  };

  KV.phaseBadge = function (phase) {
    return KV.el("span", { className: "badge " + KV.phaseClass(phase), text: phase || "Unknown" });
  };

  // Split a command line like a shell would, keeping quoted words whole:
  // SET greeting "hello world" -> ["SET", "greeting", "hello world"].
  KV.splitArgs = function (line) {
    var out = [];
    var cur = "";
    var quote = null;
    var any = false;
    for (var i = 0; i < line.length; i++) {
      var ch = line[i];
      if (quote) {
        if (ch === "\\" && quote === '"' && i + 1 < line.length) {
          var n = line[++i];
          cur += n === "n" ? "\n" : n === "t" ? "\t" : n;
        } else if (ch === quote) {
          quote = null;
        } else {
          cur += ch;
        }
      } else if (ch === '"' || ch === "'") {
        quote = ch;
        any = true;
      } else if (/\s/.test(ch)) {
        if (any) out.push(cur);
        cur = "";
        any = false;
      } else {
        cur += ch;
        any = true;
      }
    }
    if (any) out.push(cur);
    return out;
  };

  // ---------- HTTP ----------

  KV.fetchJSON = function (url, opts) {
    return fetch(url, Object.assign({ cache: "no-store" }, opts || {})).then(function (resp) {
      return resp
        .json()
        .catch(function () {
          return {};
        })
        .then(function (body) {
          // Signed out (the session expired): back to the login page.
          if (resp.status === 401 && document.body.dataset.page !== "login") {
            var here = window.location.pathname + window.location.search + window.location.hash;
            window.location.href = "/login?next=" + encodeURIComponent(here);
          }
          if (!resp.ok) {
            var err = new Error((body && body.error) || "HTTP " + resp.status);
            err.status = resp.status;
            throw err;
          }
          return body;
        });
    });
  };

  KV.sendJSON = function (method, url, body) {
    return KV.fetchJSON(url, {
      method: method,
      headers: { "Content-Type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  };

  KV.loadSession = function () {
    return KV.fetchJSON("/api/session")
      .then(function (s) {
        KV.session = s;
        return s;
      })
      .catch(function () {
        return KV.session;
      });
  };

  // Elements marked data-write are disabled on a read-only GUI, with the
  // reason on hover.
  KV.applyWriteMode = function (root) {
    var ro = !KV.session.writable;
    (root || document).querySelectorAll("[data-write]").forEach(function (b) {
      if (b.tagName === "A") {
        b.classList.toggle("disabled", ro);
        if (ro) b.setAttribute("aria-disabled", "true");
      } else {
        b.disabled = ro;
      }
      if (ro) b.title = "Read-only: set GUI_AUTH_USERNAME and GUI_AUTH_PASSWORD (gui.auth in the chart) to enable changes";
    });
    var pill = KV.qs("mode-pill");
    if (pill) {
      pill.textContent = ro ? "read-only" : "signed in" + (KV.session.user ? " as " + KV.session.user : "");
      pill.title = ro ? "Changes are disabled until GUI authentication is configured." : "";
      if (!ro && !KV.qs("signout-form")) {
        var form = KV.el(
          "form",
          { id: "signout-form", className: "signout", method: "post", action: "/logout" },
          KV.el("button", { type: "submit", className: "btn-ghost btn-sm", text: "Sign out" })
        );
        pill.parentNode.insertBefore(form, pill.nextSibling);
      }
    }
  };

  KV.setRefreshed = function (ok) {
    var el = KV.qs("refresh-indicator");
    if (!el) return;
    el.classList.toggle("stale", !ok);
    el.textContent = ok ? "updated " + new Date().toLocaleTimeString() : "refresh failed, retrying…";
  };

  KV.showError = function (message) {
    var b = KV.qs("error-banner");
    if (!b) return;
    b.textContent = message || "";
    b.classList.toggle("hidden", !message);
  };

  // ---------- theme ----------

  var THEME_KEY = "kividb-gui-theme";

  function storedTheme() {
    try {
      return localStorage.getItem(THEME_KEY);
    } catch (e) {
      return null;
    }
  }

  KV.applyTheme = function (theme) {
    if (!theme) {
      theme = window.matchMedia && window.matchMedia("(prefers-color-scheme: light)").matches ? "light" : "dark";
    }
    document.documentElement.setAttribute("data-theme", theme);
    var btn = KV.qs("theme-toggle");
    if (btn) {
      btn.textContent = theme === "light" ? "☾" : "☀";
      btn.title = theme === "light" ? "Dark theme" : "Light theme";
    }
    document.dispatchEvent(new CustomEvent("kv-theme"));
  };

  KV.applyTheme(storedTheme());

  function wireTheme() {
    KV.applyTheme(storedTheme());
    var btn = KV.qs("theme-toggle");
    if (!btn) return;
    btn.addEventListener("click", function () {
      var next = document.documentElement.getAttribute("data-theme") === "light" ? "dark" : "light";
      try {
        localStorage.setItem(THEME_KEY, next);
      } catch (e) {}
      KV.applyTheme(next);
    });
  }

  // ---------- toasts ----------

  KV.toast = function (message, kind) {
    var host = KV.qs("toasts");
    if (!host) {
      host = KV.el("div", { id: "toasts", className: "toasts", role: "status", "aria-live": "polite" });
      document.body.appendChild(host);
    }
    var t = KV.el("div", { className: "toast " + (kind || "ok"), text: message });
    host.appendChild(t);
    setTimeout(function () {
      t.remove();
    }, kind === "error" ? 9000 : 5000);
  };

  // ---------- dialogs ----------

  // KV.dialog({title, message, confirmText, danger, typeToConfirm, choices,
  // body}) resolves to false (cancelled), true, or the chosen value.
  KV.dialog = function (opts) {
    return new Promise(function (resolve) {
      var prevFocus = document.activeElement;
      var backdrop = KV.el("div", { className: "dialog-backdrop" });
      var box = KV.el("div", { className: "dialog", role: "dialog", "aria-modal": "true" });
      box.appendChild(KV.el("h3", { text: opts.title }));
      if (opts.message) box.appendChild(KV.el("p", { text: opts.message }));
      if (opts.body) box.appendChild(opts.body);

      var input = null;
      if (opts.typeToConfirm) {
        box.appendChild(KV.el("p", null, "Type ", KV.el("code", { text: opts.typeToConfirm }), " to confirm."));
        input = KV.el("input", { type: "text", autocomplete: "off", spellcheck: "false" });
        box.appendChild(input);
      }

      var chosen = null;
      if (opts.choices && opts.choices.length) {
        var list = KV.el("div", { className: "choices" });
        opts.choices.forEach(function (c, i) {
          var radio = KV.el("input", { type: "radio", name: "kv-choice", value: c.value });
          if (i === 0) {
            radio.checked = true;
            chosen = c.value;
          }
          radio.addEventListener("change", function () {
            chosen = c.value;
          });
          list.appendChild(KV.el("label", null, radio, c.label, c.note ? KV.el("small", { text: c.note }) : null));
        });
        box.appendChild(list);
      }

      var ok = KV.el("button", {
        type: "button",
        className: opts.danger ? "btn-danger solid" : "btn-primary",
        text: opts.confirmText || "Confirm",
      });
      var cancel = KV.el("button", { type: "button", className: "btn-secondary", text: "Cancel" });
      box.appendChild(KV.el("div", { className: "dialog-actions" }, cancel, ok));
      backdrop.appendChild(box);
      document.body.appendChild(backdrop);

      function close(result) {
        document.removeEventListener("keydown", onKey);
        backdrop.remove();
        if (prevFocus && prevFocus.focus) prevFocus.focus();
        resolve(result);
      }
      function confirm() {
        if (ok.disabled) return;
        close(opts.choices ? chosen : true);
      }
      function onKey(ev) {
        if (ev.key === "Escape") close(false);
        if (ev.key === "Enter" && ev.target.tagName !== "BUTTON") confirm();
      }
      if (input) {
        ok.disabled = true;
        input.addEventListener("input", function () {
          ok.disabled = input.value !== opts.typeToConfirm;
        });
      }
      ok.addEventListener("click", confirm);
      cancel.addEventListener("click", function () {
        close(false);
      });
      backdrop.addEventListener("click", function (ev) {
        if (ev.target === backdrop) close(false);
      });
      document.addEventListener("keydown", onKey);
      (input || ok).focus();
    });
  };

  // ---------- charts ----------

  var SVGNS = "http://www.w3.org/2000/svg";

  function svgEl(tag, attrs) {
    var n = document.createElementNS(SVGNS, tag);
    Object.keys(attrs || {}).forEach(function (k) {
      n.setAttribute(k, attrs[k]);
    });
    return n;
  }

  KV.seriesColor = function (i) {
    return getComputedStyle(document.documentElement).getPropertyValue("--series-" + ((i % 6) + 1)).trim() || "#f5bc18";
  };

  // "Nice" axis ticks for [min, max]: 1, 2 or 5 times a power of ten.
  function niceTicks(min, max, count) {
    if (!(max > min)) {
      max = min + 1;
    }
    var span = max - min;
    var step = Math.pow(10, Math.floor(Math.log10(span / count)));
    var err = (count * step) / span;
    if (err <= 0.15) step *= 10;
    else if (err <= 0.35) step *= 5;
    else if (err <= 0.75) step *= 2;
    var lo = Math.floor(min / step) * step;
    var hi = Math.ceil(max / step) * step;
    var ticks = [];
    for (var v = lo; v <= hi + step / 2; v += step) ticks.push(Number(v.toPrecision(12)));
    return { lo: lo, hi: hi, ticks: ticks };
  }

  // A sparkline into an <svg>: no axes, a soft area under the line.
  KV.sparkline = function (svg, pts, color) {
    KV.clear(svg);
    var w = 120,
      h = 34;
    svg.setAttribute("viewBox", "0 0 " + w + " " + h);
    svg.setAttribute("preserveAspectRatio", "none");
    if (!pts || pts.length < 2) {
      svg.appendChild(svgEl("line", { x1: 0, y1: h - 1, x2: w, y2: h - 1, stroke: "var(--grid)", "stroke-width": 2 }));
      return;
    }
    var min = Infinity,
      max = -Infinity;
    pts.forEach(function (p) {
      min = Math.min(min, p.v);
      max = Math.max(max, p.v);
    });
    if (min > 0 && min / (max || 1) > 0.5) min = min * 0.9;
    else min = Math.min(0, min);
    if (max === min) max = min + 1;
    var t0 = pts[0].t,
      t1 = pts[pts.length - 1].t || t0 + 1;
    var d = "";
    pts.forEach(function (p, i) {
      var x = ((p.t - t0) / (t1 - t0 || 1)) * w;
      var y = h - 2 - ((p.v - min) / (max - min)) * (h - 4);
      d += (i ? "L" : "M") + x.toFixed(1) + " " + y.toFixed(1);
    });
    var area = svgEl("path", { d: d + "L" + w + " " + h + "L0 " + h + "Z", fill: color, opacity: 0.15 });
    var line = svgEl("path", { d: d, fill: "none", stroke: color, "stroke-width": 1.6, "vector-effect": "non-scaling-stroke" });
    svg.appendChild(area);
    svg.appendChild(line);
  };

  // KV.lineChart(container, {series: [{name, points:[{t,v}]}], format,
  // from, to}) draws a chart with axes, a legend and a hover tooltip.
  KV.lineChart = function (container, opts) {
    KV.clear(container);
    var series = (opts.series || []).filter(function (s) {
      return s.points && s.points.length;
    });
    var fmt = opts.format || KV.fmtNum;
    var W = Math.max(container.clientWidth, 260),
      H = opts.height || 190;
    var padL = 52,
      padR = 10,
      padT = 8,
      padB = 22;
    var svg = svgEl("svg", { viewBox: "0 0 " + W + " " + H });
    svg.style.height = H + "px";
    container.appendChild(svg);

    if (!series.length) {
      var msg = svgEl("text", { x: W / 2, y: H / 2, "text-anchor": "middle", class: "empty-msg" });
      msg.textContent = opts.emptyText || "No data yet: the GUI scrapes every 15 s.";
      svg.appendChild(msg);
      return;
    }

    var tMin = opts.from || Infinity,
      tMax = opts.to || -Infinity,
      vMin = Infinity,
      vMax = -Infinity;
    series.forEach(function (s) {
      s.points.forEach(function (p) {
        if (!opts.from) tMin = Math.min(tMin, p.t);
        if (!opts.to) tMax = Math.max(tMax, p.t);
        vMin = Math.min(vMin, p.v);
        vMax = Math.max(vMax, p.v);
      });
    });
    if (tMax <= tMin) tMax = tMin + 1;
    if (opts.zeroBased !== false) vMin = Math.min(0, vMin);
    var yt = niceTicks(vMin, vMax === vMin ? vMin + 1 : vMax, 4);
    var x = function (t) {
      return padL + ((t - tMin) / (tMax - tMin)) * (W - padL - padR);
    };
    var y = function (v) {
      return H - padB - ((v - yt.lo) / (yt.hi - yt.lo || 1)) * (H - padT - padB);
    };

    var axis = svgEl("g", { class: "axis" });
    yt.ticks.forEach(function (v) {
      axis.appendChild(svgEl("line", { class: "gridline", x1: padL, x2: W - padR, y1: y(v), y2: y(v) }));
      var label = svgEl("text", { x: padL - 6, y: y(v) + 3, "text-anchor": "end" });
      label.textContent = fmt(v);
      axis.appendChild(label);
    });
    var span = tMax - tMin;
    var withDate = span > 20 * 3600 * 1000;
    var nx = Math.max(2, Math.min(6, Math.floor((W - padL) / 90)));
    for (var i = 0; i <= nx; i++) {
      var tt = tMin + (span * i) / nx;
      var tl = svgEl("text", { x: x(tt), y: H - 6, "text-anchor": i === 0 ? "start" : i === nx ? "end" : "middle" });
      tl.textContent = KV.fmtClock(tt, withDate && i === 0);
      axis.appendChild(tl);
    }
    svg.appendChild(axis);

    series.forEach(function (s, i) {
      var color = s.color || KV.seriesColor(i);
      s._color = color;
      var d = "";
      var gap = (opts.gapMs || 90000);
      s.points.forEach(function (p, j) {
        var jump = j === 0 || p.t - s.points[j - 1].t > gap;
        d += (jump ? "M" : "L") + x(p.t).toFixed(1) + " " + y(p.v).toFixed(1);
      });
      if (series.length === 1) {
        var base = y(Math.max(yt.lo, 0));
        var first = s.points[0],
          last = s.points[s.points.length - 1];
        svg.appendChild(
          svgEl("path", { class: "area", fill: color, d: d + "L" + x(last.t) + " " + base + "L" + x(first.t) + " " + base + "Z" })
        );
      }
      svg.appendChild(svgEl("path", { class: "series", stroke: color, d: d }));
    });

    if (series.length > 1 || opts.legend) {
      var legend = KV.el("div", { className: "legend" });
      series.forEach(function (s) {
        var sw = KV.el("i");
        sw.style.background = s._color;
        legend.appendChild(KV.el("span", null, sw, " " + s.name));
      });
      container.appendChild(legend);
    }

    // Hover: a cursor line and every series' value at the nearest time.
    var cursor = svgEl("line", { class: "cursor", y1: padT, y2: H - padB, visibility: "hidden" });
    svg.appendChild(cursor);
    var tip = KV.el("div", { className: "chart-tip hidden" });
    container.appendChild(tip);
    function nearest(pts, t) {
      var lo = 0,
        hi = pts.length - 1;
      while (hi - lo > 1) {
        var mid = (lo + hi) >> 1;
        if (pts[mid].t < t) lo = mid;
        else hi = mid;
      }
      return Math.abs(pts[lo].t - t) <= Math.abs(pts[hi].t - t) ? pts[lo] : pts[hi];
    }
    svg.addEventListener("mousemove", function (ev) {
      var r = svg.getBoundingClientRect();
      var px = ((ev.clientX - r.left) / r.width) * W;
      if (px < padL || px > W - padR) {
        cursor.setAttribute("visibility", "hidden");
        tip.classList.add("hidden");
        return;
      }
      var t = tMin + ((px - padL) / (W - padL - padR)) * (tMax - tMin);
      cursor.setAttribute("x1", px);
      cursor.setAttribute("x2", px);
      cursor.setAttribute("visibility", "visible");
      KV.clear(tip);
      tip.appendChild(KV.el("div", { className: "t", text: KV.fmtClock(t, withDate) }));
      series.forEach(function (s) {
        var p = nearest(s.points, t);
        if (Math.abs(p.t - t) > (opts.gapMs || 90000)) return;
        var sw = KV.el("i");
        sw.style.background = s._color;
        tip.appendChild(KV.el("div", { className: "row" }, KV.el("span", null, sw, " " + s.name), KV.el("b", { text: fmt(p.v) })));
      });
      tip.classList.remove("hidden");
      var left = ((ev.clientX - r.left) / r.width) * container.clientWidth;
      var tw = tip.offsetWidth;
      tip.style.left = (left + tw + 16 > container.clientWidth ? left - tw - 12 : left + 12) + "px";
      tip.style.top = "6px";
    });
    svg.addEventListener("mouseleave", function () {
      cursor.setAttribute("visibility", "hidden");
      tip.classList.add("hidden");
    });
  };

  // Per-pod counter samples -> per-second rate between scrapes.
  KV.rate = function (pts) {
    var out = [];
    for (var i = 1; i < (pts || []).length; i++) {
      var dt = (pts[i].t - pts[i - 1].t) / 1000;
      var dv = pts[i].v - pts[i - 1].v;
      if (dt > 0 && dv >= 0) out.push({ t: pts[i].t, v: dv / dt });
    }
    return out;
  };

  document.addEventListener("DOMContentLoaded", function () {
    wireTheme();
  });
})();
