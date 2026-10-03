// KiviDB operator GUI -- vanilla JS, no build step.
(function () {
  "use strict";

  var REFRESH_MS = 10000;
  var lastDetail = null;
  var lastLive = [];

  function qs(id) {
    return document.getElementById(id);
  }

  function phaseClass(phase) {
    switch (phase) {
      case "Running":
        return "phase-green";
      case "Degraded":
      case "Provisioning":
      case "FailingOver":
      case "Bootstrapping":
        return "phase-yellow";
      case "Error":
        return "phase-red";
      default:
        return "phase-neutral";
    }
  }

  function fmtTime(iso) {
    if (!iso) return "–";
    var d = new Date(iso);
    if (isNaN(d.getTime())) return "–";
    return d.toLocaleString();
  }

  function textOrDash(v) {
    if (v === null || v === undefined || v === "") return "–";
    return v;
  }

  function fmtBytes(n) {
    n = Number(n);
    if (!isFinite(n) || n < 0) return "–";
    if (n < 1024) return n + " B";
    if (n < 1048576) return (n / 1024).toFixed(1) + " KiB";
    if (n < 1073741824) return (n / 1048576).toFixed(1) + " MiB";
    return (n / 1073741824).toFixed(2) + " GiB";
  }

  function td(value) {
    var cell = document.createElement("td");
    cell.textContent = textOrDash(value);
    return cell;
  }

  function tr(cells) {
    var row = document.createElement("tr");
    for (var i = 0; i < cells.length; i++) row.appendChild(cells[i]);
    return row;
  }

  function emptyRow(colSpan, message) {
    var row = document.createElement("tr");
    var cell = document.createElement("td");
    cell.colSpan = colSpan;
    cell.className = "muted";
    cell.textContent = message;
    row.appendChild(cell);
    return row;
  }

  function showError(message) {
    var banner = qs("error-banner");
    if (!banner) return;
    if (!message) {
      banner.classList.add("hidden");
      banner.textContent = "";
      return;
    }
    banner.textContent = message;
    banner.classList.remove("hidden");
  }

  function showOk(message) {
    var banner = qs("ok-banner");
    if (!banner) return;
    if (!message) {
      banner.classList.add("hidden");
      banner.textContent = "";
      return;
    }
    banner.textContent = message;
    banner.classList.remove("hidden");
    setTimeout(function () {
      banner.classList.add("hidden");
    }, 5000);
  }

  function setRefreshIndicator(ok) {
    var indicator = qs("refresh-indicator");
    if (!indicator) return;
    indicator.textContent = ok
      ? "updated " + new Date().toLocaleTimeString()
      : "refresh failed, retrying…";
  }

  function fetchJSON(url, opts) {
    return fetch(url, Object.assign({ cache: "no-store" }, opts || {})).then(function (resp) {
      return resp
        .json()
        .catch(function () {
          return {};
        })
        .then(function (body) {
          if (!resp.ok) {
            var suffix = body && body.error ? ": " + body.error : "";
            throw new Error("HTTP " + resp.status + suffix);
          }
          return body;
        });
    });
  }

  function postJSON(url, body) {
    return fetchJSON(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body || {}),
    });
  }

  function pathParts() {
    var parts = window.location.pathname.split("/").filter(Boolean);
    return {
      namespace: decodeURIComponent(parts[1] || ""),
      name: decodeURIComponent(parts[2] || ""),
    };
  }

  function apiBase() {
    var p = pathParts();
    return "/api/clusters/" + encodeURIComponent(p.namespace) + "/" + encodeURIComponent(p.name);
  }

  // ---------------- Dashboard ----------------

  function renderDashboard(clusters) {
    var wrap = qs("cluster-cards");
    var empty = qs("empty-state");
    wrap.innerHTML = "";
    if (!clusters || clusters.length === 0) {
      empty.classList.remove("hidden");
      return;
    }
    empty.classList.add("hidden");
    clusters.forEach(function (c) {
      var a = document.createElement("a");
      a.className = "cluster-card";
      a.href = "/clusters/" + encodeURIComponent(c.namespace) + "/" + encodeURIComponent(c.name);
      var h = document.createElement("h3");
      h.textContent = c.name;
      var ns = document.createElement("div");
      ns.className = "ns";
      ns.textContent = c.namespace;
      var badge = document.createElement("span");
      badge.className = "badge " + phaseClass(c.phase);
      badge.textContent = c.phase || "Unknown";
      var meta = document.createElement("div");
      meta.className = "meta";
      meta.innerHTML =
        "<div><span>Master</span><br>" +
        textOrDash(c.masterPod) +
        "</div><div><span>Pods</span><br>" +
        c.readyPods +
        " / " +
        c.totalPods +
        "</div><div><span>Backup</span><br>" +
        (c.backupEnabled ? fmtTime(c.backupLastSuccess) : "disabled") +
        "</div><div><span>Age</span><br>" +
        textOrDash(c.age) +
        "</div>";
      a.appendChild(h);
      a.appendChild(ns);
      a.appendChild(badge);
      a.appendChild(meta);
      wrap.appendChild(a);
    });
  }

  function refreshDashboard() {
    fetchJSON("/api/clusters")
      .then(function (clusters) {
        renderDashboard(clusters);
        showError("");
        setRefreshIndicator(true);
      })
      .catch(function (err) {
        showError("Failed to load clusters: " + err.message);
        setRefreshIndicator(false);
      });
  }

  // ---------------- Detail ----------------

  function setFacts(id, pairs) {
    var el = qs(id);
    if (!el) return;
    el.innerHTML = "";
    pairs.forEach(function (pair) {
      var d = document.createElement("div");
      d.className = "fact";
      var k = document.createElement("div");
      k.className = "k";
      k.textContent = pair[0];
      var v = document.createElement("div");
      v.className = "v";
      v.textContent = textOrDash(pair[1]);
      d.appendChild(k);
      d.appendChild(v);
      el.appendChild(d);
    });
  }

  function fillPodSelects(pods) {
    ["log-pod", "exec-pod"].forEach(function (id) {
      var sel = qs(id);
      if (!sel) return;
      var cur = sel.value;
      sel.innerHTML = "";
      (pods || []).forEach(function (p) {
        var o = document.createElement("option");
        o.value = p.name;
        o.textContent = p.name + " (" + (p.role || "?") + ")";
        sel.appendChild(o);
      });
      if (cur) sel.value = cur;
    });
  }

  function renderDetail(d) {
    lastDetail = d;
    document.title = "KiviDB — " + d.name;
    qs("cluster-title").textContent = d.namespace + " / " + d.name;

    var badge = qs("phase-badge");
    badge.textContent = d.phase || "Unknown";
    badge.className = "badge " + phaseClass(d.phase);

    qs("w-phase").textContent = d.phase || "–";
    qs("w-master").textContent = d.masterPod || "–";
    qs("w-pods").textContent = d.readyPods + " / " + d.totalPods;
    qs("w-replicas").textContent = (d.replicas != null ? d.replicas : Math.max((d.desiredPods || 1) - 1, 0)) + " replicas + 1 master";
    qs("w-age").textContent = d.age || "–";

    setFacts("spec-facts", [
      ["Image", d.image],
      ["Agent image", d.agentImage],
      ["Port", d.port],
      ["Replicas", d.replicas],
      ["Desired pods", d.desiredPods],
      ["Storage", d.storageSize + (d.storageClassName ? " (" + d.storageClassName + ")" : "")],
      ["Master service", d.masterServiceType],
      ["Replica service", d.replicaServiceType],
      ["Config", d.configRef],
      ["ACL", d.aclConfigRef],
      ["Last failover", fmtTime(d.lastFailoverTime)],
    ]);

    if (d.backupEnabled) {
      setFacts("backup-facts", [
        ["Enabled", "yes"],
        ["Config", d.snapshotConfigRef],
        ["Schedule", d.backupSchedule],
        ["Retention", d.backupRetention],
        ["Last success", fmtTime(d.backupLastSuccess)],
        ["Last error", d.backupLastError],
      ]);
    } else {
      setFacts("backup-facts", [["Enabled", "no"], ["Hint", "Set spec.snapshotConfigRef to enable snapshots"]]);
    }

    var servicesBody = qs("services-table").querySelector("tbody");
    servicesBody.innerHTML = "";
    var services = d.services || [];
    if (!services.length) {
      servicesBody.appendChild(emptyRow(5, "No Service objects yet."));
    } else {
      services.forEach(function (s) {
        servicesBody.appendChild(tr([td(s.name), td(s.type), td(s.clusterIP), td(s.externalIP), td((s.ports || []).join(", "))]));
      });
    }

    var podsBody = qs("pods-table").querySelector("tbody");
    podsBody.innerHTML = "";
    var pods = d.pods || [];
    if (!pods.length) {
      podsBody.appendChild(emptyRow(8, "No pods reported yet."));
    } else {
      pods.forEach(function (p) {
        var actions = document.createElement("td");
        actions.className = "row-actions";
        var restart = document.createElement("button");
        restart.type = "button";
        restart.className = "btn-secondary btn-sm";
        restart.textContent = "Restart";
        restart.onclick = function () {
          restartPod(p.name);
        };
        actions.appendChild(restart);
        if (p.role === "replica") {
          var promo = document.createElement("button");
          promo.type = "button";
          promo.className = "btn-primary btn-sm";
          promo.textContent = "Promote";
          promo.onclick = function () {
            promotePod(p.name);
          };
          actions.appendChild(promo);
        }
        podsBody.appendChild(
          tr([td(p.name), td(p.role), td(p.ready ? "yes" : "no"), td(p.phase), td(p.replicationOffset), td(p.nodeName), td(p.restartCount), actions])
        );
      });
    }

    var condBody = qs("conditions-table").querySelector("tbody");
    condBody.innerHTML = "";
    var conditions = d.conditions || [];
    if (!conditions.length) {
      condBody.appendChild(emptyRow(4, "No conditions."));
    } else {
      conditions.forEach(function (c) {
        condBody.appendChild(tr([td(c.type), td(c.status), td(c.reason), td(c.message)]));
      });
    }

    var eventsBody = qs("events-table").querySelector("tbody");
    eventsBody.innerHTML = "";
    var events = d.events || [];
    if (!events.length) {
      eventsBody.appendChild(emptyRow(5, "No recent events."));
    } else {
      events.forEach(function (e) {
        eventsBody.appendChild(tr([td(fmtTime(e.lastTimestamp)), td(e.type), td(e.reason), td(e.count), td(e.message)]));
      });
    }

    var snapBody = qs("snapshots-table").querySelector("tbody");
    snapBody.innerHTML = "";
    var snaps = d.snapshots || [];
    if (!snaps.length) {
      snapBody.appendChild(emptyRow(5, "No snapshots."));
    } else {
      snaps.forEach(function (s) {
        snapBody.appendChild(
          tr([td(s.name), td(s.phase), td((s.sourcePod || "") + (s.sourceRole ? " (" + s.sourceRole + ")" : "")), td(s.objectKey), td(s.sizeBytes ? fmtBytes(s.sizeBytes) : "–")])
        );
      });
    }

    fillPodSelects(pods);
  }

  function applyLiveWidgets(rows) {
    lastLive = rows || [];
    var mem = 0;
    var haveMem = false;
    lastLive.forEach(function (r) {
      var n = Number(r.usedMemory);
      if (isFinite(n)) {
        mem += n;
        haveMem = true;
      }
    });
    qs("w-mem").textContent = haveMem ? fmtBytes(mem) : "–";
  }

  function refreshOps() {
    var base = apiBase();
    fetchJSON(base + "/live")
      .then(function (rows) {
        applyLiveWidgets(rows);
        var body = qs("live-table").querySelector("tbody");
        body.innerHTML = "";
        if (!rows.length) {
          body.appendChild(emptyRow(6, "No live data."));
          return;
        }
        rows.forEach(function (r) {
          body.appendChild(
            tr([td(r.pod), td(r.role), td(r.ready ? "yes" : "no"), td(r.replicationOffset), td(r.masterHost), td(r.usedMemory ? fmtBytes(r.usedMemory) : r.error)])
          );
        });
      })
      .catch(function () {});
    fetchJSON(base + "/dbops")
      .then(function (items) {
        var body = qs("dbops-table").querySelector("tbody");
        body.innerHTML = "";
        if (!items.length) {
          body.appendChild(emptyRow(5, "No DbOps yet."));
          return;
        }
        items.forEach(function (op) {
          body.appendChild(
            tr([
              td(op.metadata.name),
              td(op.spec.op),
              td(op.status && op.status.phase),
              td(op.status && (op.status.message || op.status.error)),
              td(op.metadata.creationTimestamp),
            ])
          );
        });
      })
      .catch(function () {});
    refreshMetricsCharts(base);
  }

  function refreshMetricsCharts(base) {
    fetchJSON(base + "/metrics")
      .then(function (payload) {
        var pods = payload.pods || {};
        var mem = [],
          clients = [],
          repl = [],
          cmds = [];
        var lastClients = 0;
        var haveClients = false;
        Object.keys(pods).forEach(function (pod) {
          var s = pods[pod];
          if (s.memory) mem = mem.concat(s.memory);
          if (s.clients) {
            clients = clients.concat(s.clients);
            if (s.clients.length) {
              lastClients += s.clients[s.clients.length - 1].v;
              haveClients = true;
            }
          }
          if (s.replOffset) repl = repl.concat(s.replOffset);
          if (s.commands) cmds = cmds.concat(s.commands);
        });
        if (haveClients) qs("w-clients").textContent = String(Math.round(lastClients));
        drawSparkline("chart-memory", aggregateSeries(mem));
        drawSparkline("chart-clients", aggregateSeries(clients));
        drawSparkline("chart-repl", aggregateSeries(repl));
        drawSparkline("chart-ops", counterToRate(aggregateSeries(cmds)));
      })
      .catch(function () {});
  }

  function aggregateSeries(pts) {
    if (!pts || !pts.length) return [];
    var buckets = {};
    pts.forEach(function (p) {
      var t = Math.floor(p.t / 15000) * 15000;
      buckets[t] = (buckets[t] || 0) + p.v;
    });
    return Object.keys(buckets)
      .map(Number)
      .sort(function (a, b) {
        return a - b;
      })
      .map(function (t) {
        return { t: t, v: buckets[t] };
      });
  }

  function counterToRate(pts) {
    if (!pts || pts.length < 2) return [];
    var out = [];
    for (var i = 1; i < pts.length; i++) {
      var dt = (pts[i].t - pts[i - 1].t) / 1000;
      if (dt <= 0) continue;
      var dv = pts[i].v - pts[i - 1].v;
      if (dv < 0) dv = 0;
      out.push({ t: pts[i].t, v: dv / dt });
    }
    return out;
  }

  function drawSparkline(svgId, pts) {
    var svg = qs(svgId);
    if (!svg) return;
    while (svg.firstChild) svg.removeChild(svg.firstChild);
    if (!pts || pts.length < 2) {
      var empty = document.createElementNS("http://www.w3.org/2000/svg", "text");
      empty.setAttribute("x", "8");
      empty.setAttribute("y", "58");
      empty.setAttribute("fill", "#a3a3a3");
      empty.setAttribute("font-size", "12");
      empty.textContent = "collecting…";
      svg.appendChild(empty);
      return;
    }
    var w = 320,
      h = 110,
      pad = 6;
    var min = pts[0].v,
      max = pts[0].v;
    pts.forEach(function (p) {
      if (p.v < min) min = p.v;
      if (p.v > max) max = p.v;
    });
    if (min === max) {
      min -= 1;
      max += 1;
    }
    var t0 = pts[0].t,
      t1 = pts[pts.length - 1].t;
    if (t1 === t0) t1 = t0 + 1;
    var d = "";
    pts.forEach(function (p, i) {
      var x = pad + ((p.t - t0) / (t1 - t0)) * (w - 2 * pad);
      var y = h - pad - ((p.v - min) / (max - min)) * (h - 2 * pad);
      d += (i === 0 ? "M" : "L") + x.toFixed(1) + " " + y.toFixed(1);
    });
    var path = document.createElementNS("http://www.w3.org/2000/svg", "path");
    path.setAttribute("d", d);
    path.setAttribute("fill", "none");
    path.setAttribute("stroke", "#f5bc18");
    path.setAttribute("stroke-width", "1.8");
    svg.appendChild(path);
  }

  function refreshDetail() {
    var parts = pathParts();
    fetchJSON("/api/clusters/" + encodeURIComponent(parts.namespace) + "/" + encodeURIComponent(parts.name))
      .then(function (detail) {
        renderDetail(detail);
        showError("");
        setRefreshIndicator(true);
      })
      .catch(function (err) {
        showError("Failed to load cluster: " + err.message);
        setRefreshIndicator(false);
      });
    refreshOps();
  }

  function restartPod(pod) {
    if (!window.confirm("Restart pod " + pod + "? Kubernetes will recreate it.")) return;
    fetch(apiBase() + "/pods/" + encodeURIComponent(pod), { method: "DELETE" })
      .then(function (r) {
        return r.json().then(function (j) {
          if (!r.ok) throw new Error(j.error || r.statusText);
          return j;
        });
      })
      .then(function () {
        showOk("Restarting " + pod);
        refreshDetail();
      })
      .catch(function (e) {
        showError(e.message);
      });
  }

  function promotePod(pod) {
    if (!window.confirm("Promote " + pod + " to master? Other pods will be pointed at it.")) return;
    postJSON(apiBase() + "/promote", { pod: pod })
      .then(function () {
        showOk("Promoted " + pod);
        refreshDetail();
      })
      .catch(function (e) {
        showError(e.message);
      });
  }

  function scaleTo(replicas) {
    if (replicas < 0) return;
    postJSON(apiBase() + "/scale", { replicas: replicas })
      .then(function (j) {
        showOk("Scaling to " + j.replicas + " replicas (" + j.desiredPods + " pods)");
        refreshDetail();
      })
      .catch(function (e) {
        showError(e.message);
      });
  }

  function currentReplicas() {
    if (lastDetail && lastDetail.replicas != null) return lastDetail.replicas;
    return 0;
  }

  function wireTabs() {
    document.querySelectorAll(".tab").forEach(function (tab) {
      tab.addEventListener("click", function () {
        var id = tab.getAttribute("data-tab");
        document.querySelectorAll(".tab").forEach(function (t) {
          t.classList.toggle("active", t === tab);
        });
        document.querySelectorAll(".tab-panel").forEach(function (p) {
          p.classList.toggle("hidden", p.id !== "panel-" + id);
        });
      });
    });
  }

  function wireOps() {
    var base = apiBase();
    qs("btn-restart").addEventListener("click", function () {
      if (!window.confirm("Create an InPlace rolling restart (replicas first, then master)?")) return;
      postJSON(base + "/restart", {})
        .then(function () {
          showOk("Rolling restart started");
          refreshOps();
        })
        .catch(function (e) {
          showError(e.message);
        });
    });
    qs("btn-scale-up").addEventListener("click", function () {
      scaleTo(currentReplicas() + 1);
    });
    qs("btn-scale-down").addEventListener("click", function () {
      var n = currentReplicas();
      if (n <= 0) {
        showError("Already at 0 replicas (master-only).");
        return;
      }
      if (!window.confirm("Remove one replica? (spec.replicas " + n + " → " + (n - 1) + ")")) return;
      scaleTo(n - 1);
    });
    qs("btn-promote").addEventListener("click", function () {
      var pods = (lastDetail && lastDetail.pods) || [];
      var replicas = pods.filter(function (p) {
        return p.role === "replica";
      });
      if (!replicas.length) {
        showError("No replica pods to promote.");
        return;
      }
      var names = replicas.map(function (p) {
        return p.name;
      });
      var pick = window.prompt("Promote which replica?\n" + names.join("\n"), names[0]);
      if (!pick) return;
      promotePod(pick.trim());
    });
    qs("btn-snapshot").addEventListener("click", function () {
      if (!window.confirm("Trigger an on-demand snapshot from the current master?")) return;
      postJSON(base + "/snapshot", {})
        .then(function (j) {
          showOk("Snapshot uploaded" + (j.objectKey ? ": " + j.objectKey : ""));
          refreshDetail();
        })
        .catch(function (e) {
          showError(e.message);
        });
    });
    qs("btn-delete").addEventListener("click", function () {
      var p = pathParts();
      var typed = window.prompt("This deletes KividbCluster " + p.name + ".\nType the cluster name to confirm:");
      if (typed !== p.name) return;
      fetch(base, { method: "DELETE" })
        .then(function (r) {
          return r.json().then(function (j) {
            if (!r.ok) throw new Error(j.error || r.statusText);
            return j;
          });
        })
        .then(function () {
          window.location.href = "/";
        })
        .catch(function (e) {
          showError(e.message);
        });
    });
    qs("btn-logs").addEventListener("click", function () {
      var pod = qs("log-pod").value;
      var c = qs("log-container").value;
      fetch(base + "/pods/" + encodeURIComponent(pod) + "/logs?container=" + encodeURIComponent(c) + "&tail=500")
        .then(function (r) {
          return r.text().then(function (t) {
            if (!r.ok) throw new Error(t);
            return t;
          });
        })
        .then(function (t) {
          qs("log-view").textContent = t || "(empty)";
          qs("log-view").classList.remove("muted");
        })
        .catch(function (e) {
          qs("log-view").textContent = e.message;
        });
    });
    qs("btn-exec").addEventListener("click", function () {
      var args = qs("exec-cmd").value.trim().split(/\s+/).filter(Boolean);
      var dangerous = /^(FLUSHALL|FLUSHDB|SHUTDOWN|DEBUG|CONFIG)$/i;
      if (args[0] && dangerous.test(args[0]) && !window.confirm("Run dangerous command " + args[0] + "?")) return;
      postJSON(base + "/exec", { pod: qs("exec-pod").value, args: args })
        .then(function (j) {
          qs("exec-out").textContent = JSON.stringify(j, null, 2);
        })
        .catch(function (e) {
          qs("exec-out").textContent = e.message;
        });
    });
    qs("exec-cmd").addEventListener("keydown", function (ev) {
      if (ev.key === "Enter") qs("btn-exec").click();
    });
  }

  document.addEventListener("DOMContentLoaded", function () {
    var page = document.body.dataset.page;
    if (page === "dashboard") {
      refreshDashboard();
      setInterval(refreshDashboard, REFRESH_MS);
    } else if (page === "detail") {
      wireTabs();
      wireOps();
      refreshDetail();
      setInterval(refreshDetail, REFRESH_MS);
    }
  });
})();
