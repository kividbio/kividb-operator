// Dashboard: summary tiles, a filterable grid of cluster cards.
(function () {
  "use strict";
  var KV = window.KV;
  var el = KV.el;
  var clusters = null;
  var filter = "all";
  var FILTER_KEY = "kividb-gui-filter";

  function matches(c, text) {
    if (filter !== "all" && KV.phaseGroup(c.phase) !== filter) return false;
    if (!text) return true;
    var hay = [c.name, c.namespace, c.version, c.variant, c.masterPod].join(" ").toLowerCase();
    return text
      .toLowerCase()
      .split(/\s+/)
      .every(function (w) {
        return hay.indexOf(w) >= 0;
      });
  }

  function podBar(c) {
    var bar = el("div", { className: "podbar", title: c.readyPods + " of " + c.desiredPods + " pods ready" });
    var n = Math.max(c.desiredPods || 0, c.totalPods || 0);
    for (var i = 0; i < n; i++) {
      var cls = i < c.readyPods ? "ready" : "";
      if (i === 0 && c.masterPod && c.readyPods > 0) cls = "master";
      bar.appendChild(el("span", { className: cls }));
    }
    return bar;
  }

  function spark(label, value, pts, color) {
    var svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    KV.sparkline(svg, pts, color);
    return el("div", { className: "spark" }, el("div", { className: "spark-head" }, el("span", { text: label }), el("b", { text: value })), svg);
  }

  function card(c) {
    var s = c.stats;
    var href = "/clusters/" + encodeURIComponent(c.namespace) + "/" + encodeURIComponent(c.name);
    var chips = el("div", { className: "chips" });
    if (c.version) chips.appendChild(el("span", { className: "chip brand", text: c.version, title: c.image }));
    if (c.variant && c.variant !== "standard") chips.appendChild(el("span", { className: "chip", text: c.variant }));
    chips.appendChild(el("span", { className: "chip", text: c.replicas + (c.replicas === 1 ? " replica" : " replicas") }));

    var backup = c.backupEnabled ? (c.backupLastSuccess ? KV.fmtAgo(c.backupLastSuccess) : "enabled") : "off";
    var facts = el(
      "div",
      { className: "facts-mini" },
      el("div", null, el("div", { className: "k", text: "Master" }), el("div", { className: "v", text: KV.dash(c.masterPod) })),
      el("div", null, el("div", { className: "k", text: "Pods ready" }), el("div", { className: "v", text: c.readyPods + " / " + c.desiredPods }), podBar(c)),
      el("div", null, el("div", { className: "k", text: "Backups" }), el("div", { className: "v", text: backup })),
      el("div", null, el("div", { className: "k", text: "Age" }), el("div", { className: "v", text: KV.dash(c.age) }))
    );

    var sparks = el(
      "div",
      { className: "spark-row" },
      spark("Commands / s", s ? KV.fmtNum(s.opsPerSec) : "–", s && s.ops, KV.seriesColor(0)),
      spark("Memory", s ? KV.fmtBytes(s.memoryBytes) : "–", s && s.memory, KV.seriesColor(1))
    );

    return el(
      "a",
      { className: "cluster-card", href: href },
      el("div", { className: "head" }, el("div", { style: "min-width:0" }, el("h3", { text: c.name }), el("div", { className: "ns", text: c.namespace })), KV.phaseBadge(c.phase)),
      chips,
      facts,
      sparks
    );
  }

  function render() {
    if (!clusters) return;
    var text = KV.qs("search").value.trim();
    var counts = { all: clusters.length, healthy: 0, attention: 0, error: 0 };
    var mem = 0,
      ops = 0,
      clients = 0,
      pods = 0,
      ready = 0,
      haveStats = false;
    clusters.forEach(function (c) {
      counts[KV.phaseGroup(c.phase)]++;
      pods += c.desiredPods || 0;
      ready += c.readyPods || 0;
      if (c.stats) {
        haveStats = true;
        mem += c.stats.memoryBytes;
        ops += c.stats.opsPerSec;
        clients += c.stats.clients;
      }
    });
    KV.qs("t-clusters").textContent = clusters.length;
    KV.qs("t-pods").textContent = ready + " of " + pods + " pods ready";
    KV.qs("t-healthy").textContent = counts.healthy;
    KV.qs("t-attention").textContent = counts.attention;
    KV.qs("t-error").textContent = counts.error;
    KV.qs("t-memory").textContent = haveStats ? KV.fmtBytes(mem) : "–";
    KV.qs("t-ops").textContent = haveStats ? KV.fmtNum(ops) : "–";
    KV.qs("t-clients").textContent = haveStats ? KV.fmtNum(clients) + " clients connected" : "collecting…";
    ["all", "healthy", "attention", "error"].forEach(function (k) {
      KV.qs("c-" + k).textContent = counts[k];
    });

    var wrap = KV.clear(KV.qs("cluster-cards"));
    var shown = clusters.filter(function (c) {
      return matches(c, text);
    });
    shown.forEach(function (c) {
      wrap.appendChild(card(c));
    });
    KV.qs("empty-state").classList.toggle("hidden", clusters.length !== 0);
    KV.qs("no-match").classList.toggle("hidden", clusters.length === 0 || shown.length !== 0);
    KV.applyWriteMode();
  }

  function refresh() {
    KV.fetchJSON("/api/clusters")
      .then(function (list) {
        clusters = list || [];
        render();
        KV.showError("");
        KV.setRefreshed(true);
      })
      .catch(function (err) {
        KV.showError("Could not load clusters: " + err.message);
        KV.setRefreshed(false);
      });
  }

  function setFilter(f) {
    filter = f;
    document.querySelectorAll("#phase-filter button").forEach(function (b) {
      b.classList.toggle("active", b.dataset.filter === f);
    });
    try {
      sessionStorage.setItem(FILTER_KEY, f);
    } catch (e) {}
    render();
  }

  document.addEventListener("DOMContentLoaded", function () {
    try {
      filter = sessionStorage.getItem(FILTER_KEY) || "all";
    } catch (e) {}
    document.querySelectorAll("#phase-filter button").forEach(function (b) {
      b.classList.toggle("active", b.dataset.filter === filter);
      b.addEventListener("click", function () {
        setFilter(b.dataset.filter);
      });
    });
    KV.qs("search").addEventListener("input", render);
    KV.qs("btn-clear-filter").addEventListener("click", function () {
      KV.qs("search").value = "";
      setFilter("all");
    });
    document.querySelectorAll("a[data-write]").forEach(function (a) {
      a.addEventListener("click", function (ev) {
        if (a.classList.contains("disabled")) ev.preventDefault();
      });
    });
    document.addEventListener("kv-theme", render);
    KV.loadSession().then(function () {
      KV.applyWriteMode();
    });
    refresh();
    setInterval(refresh, KV.REFRESH_MS);
  });
})();
