// Cluster detail: topology, pods, charts, logs, console, backups, events,
// and the actions on a cluster.
(function () {
  "use strict";
  var KV = window.KV;
  var el = KV.el;

  var detail = null;
  var live = [];
  var metrics = null;
  var rangeMs = 3600000;
  var activeTab = "overview";
  var followTimer = null;
  var HISTORY_KEY = "kividb-gui-console-history";

  function parts() {
    var p = window.location.pathname.split("/").filter(Boolean);
    return { namespace: decodeURIComponent(p[1] || ""), name: decodeURIComponent(p[2] || "") };
  }
  function base() {
    var p = parts();
    return "/api/clusters/" + encodeURIComponent(p.namespace) + "/" + encodeURIComponent(p.name);
  }

  function td(v, cls) {
    return el("td", { className: cls, text: KV.dash(v) });
  }
  function fillTable(id, rows, cols, emptyText) {
    var body = KV.clear(KV.qs(id).querySelector("tbody"));
    if (!rows.length) {
      body.appendChild(el("tr", null, el("td", { colspan: cols, className: "muted", text: emptyText })));
      return;
    }
    rows.forEach(function (cells) {
      body.appendChild(el("tr", null, cells));
    });
  }
  function setFacts(id, pairs) {
    var box = KV.clear(KV.qs(id));
    pairs.forEach(function (p) {
      if (p[1] === undefined) return;
      box.appendChild(el("div", { className: "fact" }, el("div", { className: "k", text: p[0] }), el("div", { className: "v", text: KV.dash(p[1]) })));
    });
  }

  function liveFor(pod) {
    for (var i = 0; i < live.length; i++) if (live[i].pod === pod) return live[i];
    return null;
  }
  function masterLive() {
    for (var i = 0; i < live.length; i++) if (live[i].role === "master") return live[i];
    return detail ? liveFor(detail.masterPod) : null;
  }

  // ---------- rendering ----------

  function renderDetail() {
    var d = detail;
    document.title = d.name + " · KiviDB";
    KV.qs("crumb-ns").textContent = d.namespace;
    KV.qs("crumb-name").textContent = d.name;
    KV.qs("cluster-name").textContent = d.name;
    var badge = KV.qs("phase-badge");
    badge.textContent = d.phase || "Unknown";
    badge.className = "badge " + KV.phaseClass(d.phase);
    var vchip = KV.qs("version-chip");
    vchip.textContent = d.version || "";
    vchip.title = d.image + (d.imageDefault ? " (the operator's default)" : "");
    vchip.classList.toggle("hidden", !d.version);
    var var_ = KV.qs("variant-chip");
    var_.textContent = d.variant;
    var_.classList.toggle("hidden", !d.variant || d.variant === "standard");

    KV.qs("w-master").textContent = KV.dash(d.masterPod);
    KV.qs("w-failover").textContent = d.lastFailoverTime ? "last failover " + KV.fmtAgo(d.lastFailoverTime) : "no failover yet";
    KV.qs("w-pods").textContent = d.readyPods + " / " + d.desiredPods;
    KV.qs("w-replicas").textContent = "1 master + " + d.replicas + (d.replicas === 1 ? " replica" : " replicas");
    KV.qs("w-age").textContent = "cluster age " + KV.dash(d.age);

    setFacts("spec-facts", [
      ["Image", d.image + (d.imageDefault ? " (default)" : "")],
      ["Variant", d.variant],
      ["Agent image", d.agentImage || "operator default"],
      ["Port", d.port],
      ["Storage", d.storageSize + (d.storageClassName ? " · " + d.storageClassName : "")],
      ["Master service", d.masterServiceType || "ClusterIP"],
      ["Replica service", d.replicaServiceType || "ClusterIP"],
      ["Config", d.configRef],
      ["ACL", d.aclConfigRef],
      ["Generation observed", d.observedGeneration],
    ]);

    fillTable(
      "services-table",
      (d.services || []).map(function (s) {
        return [td(s.name, "mono"), td(s.type), td(s.clusterIP, "mono"), td(s.externalIP, "mono"), td((s.ports || []).join(", "))];
      }),
      5,
      "No Services yet."
    );
    fillTable(
      "conditions-table",
      (d.conditions || []).map(function (c) {
        var st = el("td", null, el("span", { className: "badge " + (c.status === "True" ? "phase-green" : "phase-yellow"), text: c.status }));
        return [td(c.type), st, td(c.reason), td(c.message)];
      }),
      4,
      "No conditions."
    );

    if (d.backupEnabled) {
      setFacts("backup-facts", [
        ["Snapshot config", d.snapshotConfigRef],
        ["Schedule", d.backupSchedule],
        ["Retention", d.backupRetention],
        ["Last success", d.backupLastSuccess ? KV.fmtTime(d.backupLastSuccess) + " (" + KV.fmtAgo(d.backupLastSuccess) + ")" : "none yet"],
        ["Last error", d.backupLastError || "none"],
        ["CronJob", d.cronJob ? d.cronJob.name + (d.cronJob.suspended ? " (suspended)" : "") : undefined],
      ]);
    } else {
      setFacts("backup-facts", [["Backups", "off"], ["To turn them on", "set spec.snapshotConfigRef to a KividbSnapshotConfig"]]);
    }
    fillTable(
      "snapshots-table",
      (d.snapshots || []).map(function (s) {
        var ph = el("td", null, el("span", { className: "badge " + (s.phase === "Succeeded" ? "phase-green" : s.phase === "Failed" ? "phase-red" : "phase-yellow"), text: s.phase || "?" }));
        return [
          td(s.name, "mono"),
          ph,
          td((s.sourcePod || "") + (s.sourceRole ? " (" + s.sourceRole + ")" : "")),
          td(s.sizeBytes ? KV.fmtBytes(s.sizeBytes) : null),
          td(s.durationMs ? (s.durationMs / 1000).toFixed(1) + " s" : null),
          td(s.completionTime ? KV.fmtAgo(s.completionTime) : s.error),
          td(s.objectKey, "mono"),
        ];
      }),
      7,
      d.backupEnabled ? "No snapshots yet." : "Backups are off."
    );
    fillTable(
      "events-table",
      (d.events || []).map(function (e) {
        var type = el("td", null, el("span", { className: "badge plain " + (e.type === "Warning" ? "phase-yellow" : ""), text: e.type }));
        return [td(KV.fmtAgo(e.lastTimestamp)), type, td(e.reason), td(e.count), td(e.message)];
      }),
      5,
      "No recent events."
    );

    fillPodSelects();
    renderPods();
    KV.applyWriteMode();
  }

  function fillPodSelects() {
    ["log-pod", "exec-pod"].forEach(function (id) {
      var sel = KV.qs(id);
      var cur = sel.value;
      KV.clear(sel);
      (detail.pods || []).forEach(function (p) {
        sel.appendChild(el("option", { value: p.name, text: p.name + (p.role ? " · " + p.role : "") }));
      });
      if (cur) sel.value = cur;
      else if (detail.masterPod) sel.value = detail.masterPod;
    });
  }

  function renderPods() {
    if (!detail) return;
    fillTable(
      "pods-table",
      (detail.pods || []).map(function (p) {
        var l = liveFor(p.name);
        var actions = el("td", { className: "row-actions" });
        actions.appendChild(
          el("button", {
            type: "button",
            className: "btn-secondary btn-sm",
            text: "Restart",
            "data-write": "",
            onclick: function () {
              restartPod(p.name);
            },
          })
        );
        if (p.role === "replica") {
          actions.appendChild(
            el("button", {
              type: "button",
              className: "btn-secondary btn-sm",
              text: "Promote",
              "data-write": "",
              onclick: function () {
                promote(p.name);
              },
            })
          );
        }
        var ready = el("td", null, el("span", { className: "badge " + (p.ready ? "phase-green" : "phase-red"), text: p.ready ? "yes" : "no" }));
        return [
          td(p.name, "mono"),
          td(p.role),
          ready,
          td(p.phase),
          td(l ? l.replicationOffset : p.replicationOffset, "mono"),
          td(l && l.usedMemory ? KV.fmtBytes(l.usedMemory) : null),
          td(l && l.keys != null ? KV.fmtNum(l.keys) : null),
          td(p.nodeName, "mono"),
          td(p.restartCount || 0),
          actions,
        ];
      }),
      10,
      "No pods yet."
    );
    KV.applyWriteMode(KV.qs("pods-table"));
  }

  function lagInfo(bytes) {
    if (bytes == null) return { cls: "", text: "–" };
    if (bytes <= 0) return { cls: "ok", text: "in sync" };
    if (bytes < 1048576) return { cls: bytes < 65536 ? "ok" : "warn", text: KV.fmtBytes(bytes) + " behind" };
    return { cls: "bad", text: KV.fmtBytes(bytes) + " behind" };
  }

  function topoNode(p, l, isMaster, lag) {
    var down = !p.ready || (l && l.error);
    var kv = el("div", { className: "kv" });
    function row(k, v, cls) {
      kv.appendChild(el("span", { text: k }));
      kv.appendChild(el("span", { className: cls, text: v }));
    }
    if (l && l.error) row("agent", "unreachable", "lag bad");
    if (!isMaster) {
      if (l && !l.error && !l.masterHost) row("master", "none", "lag bad");
      else row("lag", lag.text, "lag " + lag.cls);
    }
    row("memory", l && l.usedMemory ? KV.fmtBytes(l.usedMemory) : "–");
    row("keys", l && l.keys != null ? KV.fmtNum(l.keys) : "–");
    row("offset", l ? KV.fmtNum(l.replicationOffset) : KV.fmtNum(p.replicationOffset));
    row("node", KV.dash(p.nodeName));
    return el(
      "div",
      { className: "topo-node" + (isMaster ? " master" : "") + (down ? " down" : "") },
      el("div", { className: "name" }, el("span", { text: p.name }), el("span", { className: "badge " + (p.ready ? "phase-green" : "phase-red"), text: p.ready ? "ready" : "not ready" })),
      el("div", { className: "role", text: (isMaster ? "master" : "replica") + (l && l.aofEnabled ? " · AOF on" : "") }),
      kv
    );
  }

  function renderTopology() {
    var box = KV.clear(KV.qs("topology"));
    if (!detail || !(detail.pods || []).length) {
      box.appendChild(el("span", { className: "muted", text: "No pods yet." }));
      return;
    }
    var m = masterLive();
    var masterName = (m && m.pod) || detail.masterPod;
    var masterPod = null;
    var replicas = [];
    detail.pods.forEach(function (p) {
      if (p.name === masterName) masterPod = p;
      else replicas.push(p);
    });
    if (!masterPod) {
      box.appendChild(el("p", { className: "muted", text: "No master elected yet." }));
      masterPod = { name: "(none)", ready: false };
    } else {
      box.appendChild(topoNode(masterPod, m, true));
    }
    if (!replicas.length) {
      box.appendChild(el("p", { className: "muted", text: "No replicas: spec.replicas is 0." }));
      return;
    }
    box.appendChild(el("div", { className: "topo-trunk" }));
    var row = el("div", { className: "topo-replicas" });
    replicas.forEach(function (p) {
      var l = liveFor(p.name);
      var lag = m && l && !l.error ? lagInfo(Math.max(0, m.replicationOffset - l.replicationOffset)) : lagInfo(null);
      row.appendChild(el("div", { className: "topo-branch" }, topoNode(p, l, false, lag)));
    });
    box.appendChild(row);
    positionRail(row);
  }

  // The horizontal rail runs from the first replica's centre to the last's.
  function positionRail(row) {
    var kids = row.children;
    if (!kids.length) return;
    var r = row.getBoundingClientRect();
    var first = kids[0].getBoundingClientRect();
    var last = kids[kids.length - 1].getBoundingClientRect();
    if (Math.abs(first.top - last.top) > 4) {
      row.style.setProperty("--rail-left", "50%");
      row.style.setProperty("--rail-right", "50%");
      return;
    }
    row.style.setProperty("--rail-left", first.left - r.left + first.width / 2 + "px");
    row.style.setProperty("--rail-right", r.right - last.right + last.width / 2 + "px");
  }

  function renderLiveTiles() {
    var mem = 0,
      haveMem = false;
    live.forEach(function (r) {
      var n = Number(r.usedMemory);
      if (r.usedMemory && isFinite(n)) {
        mem += n;
        haveMem = true;
      }
    });
    KV.qs("w-mem").textContent = haveMem ? KV.fmtBytes(mem) : "–";
    var m = masterLive();
    KV.qs("w-keys").textContent = m && m.keys != null ? KV.fmtNum(m.keys) : "–";
  }

  // ---------- metrics ----------

  function podNames() {
    return Object.keys((metrics && metrics.pods) || {}).sort();
  }

  function lagSeries(pods, masterName) {
    var mpts = (pods[masterName] && pods[masterName].replOffset) || [];
    var out = [];
    Object.keys(pods)
      .sort()
      .forEach(function (name) {
        if (name === masterName) return;
        var rpts = pods[name].replOffset || [];
        var pts = [];
        var j = 0;
        rpts.forEach(function (p) {
          while (j + 1 < mpts.length && Math.abs(mpts[j + 1].t - p.t) <= Math.abs(mpts[j].t - p.t)) j++;
          if (mpts.length && Math.abs(mpts[j].t - p.t) <= 10000) pts.push({ t: p.t, v: Math.max(0, mpts[j].v - p.v) });
        });
        out.push({ name: name, points: pts });
      });
    return out;
  }

  function lastOf(pts) {
    return pts && pts.length ? pts[pts.length - 1].v : null;
  }

  function renderMetrics() {
    if (!metrics) return;
    var pods = metrics.pods || {};
    var names = podNames();
    var ops = names.map(function (n) {
      return { name: n, points: KV.rate(pods[n].commands) };
    });
    var mem = names.map(function (n) {
      return { name: n, points: pods[n].memory || [] };
    });
    var clients = names.map(function (n) {
      return { name: n, points: pods[n].clients || [] };
    });
    var masterName = (masterLive() || {}).pod || (detail && detail.masterPod);
    var lag = lagSeries(pods, masterName);

    var sum = function (series) {
      var t = 0,
        any = false;
      series.forEach(function (s) {
        var v = lastOf(s.points);
        if (v != null && s.points[s.points.length - 1].t > Date.now() - 60000) {
          t += v;
          any = true;
        }
      });
      return any ? t : null;
    };
    var opsNow = sum(ops),
      clientsNow = sum(clients),
      memNow = sum(mem);
    KV.qs("w-ops").textContent = opsNow == null ? "–" : KV.fmtNum(opsNow);
    KV.qs("w-clients").textContent = clientsNow == null ? "–" : KV.fmtNum(clientsNow);
    KV.qs("now-ops").textContent = opsNow == null ? "" : KV.fmtNum(opsNow) + "/s";
    KV.qs("now-clients").textContent = clientsNow == null ? "" : KV.fmtNum(clientsNow);
    KV.qs("now-mem").textContent = memNow == null ? "" : KV.fmtBytes(memNow);
    var maxLag = null;
    lag.forEach(function (s) {
      var v = lastOf(s.points);
      if (v != null) maxLag = Math.max(maxLag || 0, v);
    });
    KV.qs("now-lag").textContent = maxLag == null ? "" : lagInfo(maxLag).text;

    if (activeTab !== "metrics") return;
    var from = Date.now() - rangeMs,
      to = Date.now();
    KV.lineChart(KV.qs("chart-ops"), { series: ops, from: from, to: to, legend: true });
    KV.lineChart(KV.qs("chart-memory"), { series: mem, from: from, to: to, format: KV.fmtBytes, legend: true });
    KV.lineChart(KV.qs("chart-clients"), { series: clients, from: from, to: to, legend: true });
    KV.lineChart(KV.qs("chart-lag"), {
      series: lag,
      from: from,
      to: to,
      format: KV.fmtBytes,
      legend: true,
      emptyText: lag.length ? "No data yet: the GUI scrapes every 15 s." : "No replicas.",
    });
  }

  function refreshMetrics() {
    var from = Date.now() - rangeMs;
    return KV.fetchJSON(base() + "/metrics?from=" + from)
      .then(function (m) {
        metrics = m;
        renderMetrics();
      })
      .catch(function () {});
  }

  // ---------- refresh ----------

  function refresh() {
    var b = base();
    var pDetail = KV.fetchJSON(b)
      .then(function (d) {
        detail = d;
        renderDetail();
        KV.showError("");
        KV.setRefreshed(true);
      })
      .catch(function (err) {
        KV.showError(err.status === 404 ? "This cluster no longer exists." : "Could not load the cluster: " + err.message);
        KV.setRefreshed(false);
      });
    var pLive = KV.fetchJSON(b + "/live")
      .then(function (rows) {
        live = rows || [];
      })
      .catch(function () {});
    Promise.all([pDetail, pLive]).then(function () {
      if (!detail) return;
      renderTopology();
      renderPods();
      renderLiveTiles();
    });
    refreshMetrics();
    KV.fetchJSON(b + "/dbops")
      .then(function (items) {
        fillTable(
          "dbops-table",
          (items || []).map(function (op) {
            var st = op.status || {};
            return [td(op.metadata.name, "mono"), td(op.spec.op), td(st.phase), td(st.message || st.error), td(KV.fmtAgo(op.metadata.creationTimestamp))];
          }),
          5,
          "No operations yet."
        );
      })
      .catch(function () {});
  }

  // ---------- actions ----------

  function act(promise, okText) {
    return promise
      .then(function (j) {
        KV.toast(typeof okText === "function" ? okText(j) : okText);
        refresh();
      })
      .catch(function (e) {
        KV.toast(e.message, "error");
      });
  }

  function restartPod(pod) {
    KV.dialog({
      title: "Restart " + pod + "?",
      message:
        "Kubernetes deletes the pod and the StatefulSet recreates it." +
        (detail && pod === detail.masterPod ? " It is the master: the operator fails over to a replica first if one is ready." : ""),
      confirmText: "Restart pod",
    }).then(function (ok) {
      if (ok) act(KV.sendJSON("DELETE", base() + "/pods/" + encodeURIComponent(pod)), "Restarting " + pod);
    });
  }

  function promote(pod) {
    var m = masterLive();
    var go = function (target) {
      act(KV.sendJSON("POST", base() + "/promote", { pod: target }), "Promoted " + target + " to master");
    };
    if (pod) {
      KV.dialog({
        title: "Promote " + pod + "?",
        message: "It becomes the master and the current master becomes its replica. Writes that reach the old master in the moment between the two steps are not carried over.",
        confirmText: "Promote",
      }).then(function (ok) {
        if (ok) go(pod);
      });
      return;
    }
    var replicas = ((detail && detail.pods) || []).filter(function (p) {
      return p.role === "replica";
    });
    if (!replicas.length) {
      KV.toast("There is no replica to promote.", "info");
      return;
    }
    KV.dialog({
      title: "Promote a replica",
      message: "The chosen replica becomes the master; the current master becomes its replica.",
      confirmText: "Promote",
      choices: replicas.map(function (p) {
        var l = liveFor(p.name);
        var lag = m && l && !l.error ? lagInfo(Math.max(0, m.replicationOffset - l.replicationOffset)).text : p.ready ? "" : "not ready";
        return { value: p.name, label: p.name, note: lag };
      }),
    }).then(function (pick) {
      if (pick) go(pick);
    });
  }

  function scale(delta) {
    if (!detail) return;
    var n = detail.replicas + delta;
    if (n < 0) {
      KV.toast("Already at 0 replicas: the master runs alone.", "info");
      return;
    }
    KV.dialog({
      title: (delta > 0 ? "Add" : "Remove") + " a replica?",
      message: "spec.replicas goes from " + detail.replicas + " to " + n + ": " + (n + 1) + " pods in all." + (delta < 0 ? " The highest-numbered replica is removed; its volume stays." : ""),
      confirmText: delta > 0 ? "Add replica" : "Remove replica",
    }).then(function (ok) {
      if (ok)
        act(KV.sendJSON("POST", base() + "/scale", { replicas: n }), function (j) {
          return "Scaling to " + j.replicas + " replicas (" + j.desiredPods + " pods)";
        });
    });
  }

  function wireActions() {
    KV.qs("btn-scale-up").addEventListener("click", function () {
      scale(1);
    });
    KV.qs("btn-scale-down").addEventListener("click", function () {
      scale(-1);
    });
    KV.qs("btn-promote").addEventListener("click", function () {
      promote(null);
    });
    KV.qs("btn-snapshot").addEventListener("click", function () {
      if (detail && !detail.backupEnabled) {
        KV.toast("Backups are off for this cluster: set spec.snapshotConfigRef first.", "info");
        return;
      }
      KV.dialog({ title: "Take a snapshot now?", message: "The master's data is saved and uploaded to the configured object store.", confirmText: "Take snapshot" }).then(function (ok) {
        if (ok)
          act(KV.sendJSON("POST", base() + "/snapshot", {}), function (j) {
            return "Snapshot uploaded" + (j.objectKey ? ": " + j.objectKey : "");
          });
      });
    });
    KV.qs("btn-restart").addEventListener("click", function () {
      KV.dialog({
        title: "Rolling restart?",
        message: "Creates a KividbDbOps that restarts the pods one at a time: replicas first, then the master after a failover.",
        confirmText: "Start restart",
      }).then(function (ok) {
        if (ok) act(KV.sendJSON("POST", base() + "/restart", {}), "Rolling restart started");
      });
    });
    KV.qs("btn-delete").addEventListener("click", function () {
      var p = parts();
      KV.dialog({
        title: "Delete " + p.name + "?",
        message: "The KividbCluster and its pods are deleted. Its volumes stay until you delete their PersistentVolumeClaims.",
        confirmText: "Delete cluster",
        danger: true,
        typeToConfirm: p.name,
      }).then(function (ok) {
        if (!ok) return;
        KV.sendJSON("DELETE", base())
          .then(function () {
            window.location.href = "/";
          })
          .catch(function (e) {
            KV.toast(e.message, "error");
          });
      });
    });
  }

  // ---------- tabs ----------

  function showTab(id) {
    activeTab = id;
    document.querySelectorAll(".tab").forEach(function (t) {
      t.classList.toggle("active", t.dataset.tab === id);
    });
    document.querySelectorAll(".tab-panel").forEach(function (p) {
      p.classList.toggle("hidden", p.id !== "panel-" + id);
    });
    if (window.history.replaceState) window.history.replaceState(null, "", "#" + id);
    if (id === "metrics") renderMetrics();
    if (id === "overview") renderTopology();
    setFollow();
  }

  // ---------- logs ----------

  function escapeHTML(s) {
    return s.replace(/[&<>"]/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c];
    });
  }

  var lastLogText = "";
  function renderLogs() {
    var view = KV.qs("log-view");
    var q = KV.qs("log-filter").value;
    var atBottom = view.scrollTop + view.clientHeight >= view.scrollHeight - 30;
    var html = lastLogText
      .split("\n")
      .map(function (line) {
        var h = escapeHTML(line);
        if (q) {
          var re = new RegExp(q.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "gi");
          h = h.replace(re, function (m) {
            return "<mark>" + m + "</mark>";
          });
        }
        if (/\b(ERROR|FATAL|PANIC)\b|level=error/i.test(line)) return '<span class="err">' + h + "</span>";
        if (/\bWARN(ING)?\b|level=warn/i.test(line)) return '<span class="warn">' + h + "</span>";
        return h;
      })
      .join("\n");
    view.innerHTML = html || "(empty)";
    if (atBottom) view.scrollTop = view.scrollHeight;
  }

  function loadLogs() {
    var pod = KV.qs("log-pod").value;
    if (!pod) return;
    var url = base() + "/pods/" + encodeURIComponent(pod) + "/logs?container=" + encodeURIComponent(KV.qs("log-container").value) + "&tail=" + KV.qs("log-tail").value;
    fetch(url, { cache: "no-store" })
      .then(function (r) {
        return r.text().then(function (t) {
          if (!r.ok) throw new Error(t);
          return t;
        });
      })
      .then(function (t) {
        lastLogText = t;
        renderLogs();
      })
      .catch(function (e) {
        KV.qs("log-view").textContent = e.message;
      });
  }

  function setFollow() {
    if (followTimer) clearInterval(followTimer);
    followTimer = null;
    if (activeTab === "logs" && KV.qs("log-follow").checked) followTimer = setInterval(loadLogs, 3000);
  }

  // ---------- console ----------

  function formatReply(r, indent) {
    indent = indent || "";
    if (!r || typeof r !== "object") return String(r);
    switch (r.type) {
      case "status":
        return r.str;
      case "error":
        return "(error) " + r.str;
      case "integer":
        return "(integer) " + (r.int || 0);
      case "nil":
        return "(nil)";
      case "bulk":
        if (r.isNil) return "(nil)";
        return /\n/.test(r.str || "") ? r.str : JSON.stringify(r.str || "");
      case "array":
        if (r.isNil) return "(nil)";
        if (!r.array || !r.array.length) return "(empty array)";
        var w = String(r.array.length).length;
        return r.array
          .map(function (x, i) {
            var n = String(i + 1);
            var pre = (i ? indent : "") + " ".repeat(w - n.length) + n + ") ";
            return pre + formatReply(x, indent + " ".repeat(w + 2));
          })
          .join("\n");
    }
    return JSON.stringify(r, null, 2);
  }

  function consoleHistory() {
    try {
      return JSON.parse(sessionStorage.getItem(HISTORY_KEY) || "[]");
    } catch (e) {
      return [];
    }
  }
  var histPos = -1;

  function runCommand() {
    var input = KV.qs("exec-cmd");
    var line = input.value.trim();
    if (!line) return;
    var args = KV.splitArgs(line);
    var dangerous = /^(FLUSHALL|FLUSHDB|SHUTDOWN|DEBUG|CONFIG|SWAPDB|REPLICAOF|SLAVEOF|FAILOVER|CLUSTER)$/i;
    var go = function () {
      var h = consoleHistory().filter(function (x) {
        return x !== line;
      });
      h.unshift(line);
      try {
        sessionStorage.setItem(HISTORY_KEY, JSON.stringify(h.slice(0, 50)));
      } catch (e) {}
      histPos = -1;
      input.value = "";
      var out = KV.qs("exec-out");
      var entry = el("div", { className: "console-entry" }, el("div", { className: "cmd", text: KV.qs("exec-pod").value + " " + line }));
      var res = el("div", { text: "…" });
      entry.appendChild(res);
      out.appendChild(entry);
      out.scrollTop = out.scrollHeight;
      KV.sendJSON("POST", base() + "/exec", { pod: KV.qs("exec-pod").value, args: args })
        .then(function (j) {
          res.textContent = formatReply(j);
          if (j && j.type === "error") res.className = "err";
        })
        .catch(function (e) {
          res.textContent = "(error) " + e.message;
          res.className = "err";
        })
        .then(function () {
          out.scrollTop = out.scrollHeight;
        });
    };
    if (dangerous.test(args[0])) {
      KV.dialog({ title: "Run " + args[0].toUpperCase() + "?", message: line + "\n\nThis can lose data or change how the cluster replicates.", confirmText: "Run it", danger: true }).then(function (ok) {
        if (ok) go();
      });
    } else go();
  }

  function wireConsole() {
    KV.qs("btn-exec").addEventListener("click", runCommand);
    KV.qs("btn-exec-clear").addEventListener("click", function () {
      KV.clear(KV.qs("exec-out"));
    });
    KV.qs("exec-cmd").addEventListener("keydown", function (ev) {
      var h = consoleHistory();
      if (ev.key === "Enter") runCommand();
      else if (ev.key === "ArrowUp" && h.length) {
        histPos = Math.min(histPos + 1, h.length - 1);
        ev.target.value = h[histPos];
        ev.preventDefault();
      } else if (ev.key === "ArrowDown") {
        histPos = Math.max(histPos - 1, -1);
        ev.target.value = histPos < 0 ? "" : h[histPos];
        ev.preventDefault();
      }
    });
  }

  // ---------- start ----------

  document.addEventListener("DOMContentLoaded", function () {
    document.querySelectorAll(".tab").forEach(function (t) {
      t.addEventListener("click", function () {
        showTab(t.dataset.tab);
      });
    });
    document.querySelectorAll("#range button").forEach(function (b) {
      b.addEventListener("click", function () {
        rangeMs = Number(b.dataset.range);
        document.querySelectorAll("#range button").forEach(function (x) {
          x.classList.toggle("active", x === b);
        });
        refreshMetrics();
      });
    });
    wireActions();
    wireConsole();
    KV.qs("btn-logs").addEventListener("click", loadLogs);
    KV.qs("log-filter").addEventListener("input", renderLogs);
    KV.qs("log-follow").addEventListener("change", function () {
      setFollow();
      if (KV.qs("log-follow").checked) loadLogs();
    });
    ["log-pod", "log-container", "log-tail"].forEach(function (id) {
      KV.qs(id).addEventListener("change", loadLogs);
    });
    var resizeTimer = null;
    window.addEventListener("resize", function () {
      clearTimeout(resizeTimer);
      resizeTimer = setTimeout(function () {
        renderMetrics();
        renderTopology();
      }, 150);
    });
    document.addEventListener("kv-theme", renderMetrics);

    var hash = (window.location.hash || "").slice(1);
    if (hash && KV.qs("panel-" + hash)) showTab(hash);
    KV.loadSession().then(function () {
      KV.applyWriteMode();
    });
    refresh();
    setInterval(refresh, KV.REFRESH_MS);
  });
})();
