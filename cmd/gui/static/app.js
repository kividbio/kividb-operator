// KiviDB operator GUI -- vanilla JS, no build step, no external requests.
// Both pages (dashboard and cluster detail) share this one file and pick
// their behavior based on document.body.dataset.page.
(function () {
  "use strict";

  var REFRESH_MS = 10000;

  function qs(id) {
    return document.getElementById(id);
  }

  // phaseClass maps a KividbCluster status.phase value to a CSS status
  // color: green = healthy, yellow = transitional/degraded, red = error,
  // neutral = pending/unknown.
  function phaseClass(phase) {
    switch (phase) {
      case "Running":
        return "phase-green";
      case "Degraded":
      case "Provisioning":
      case "FailingOver":
        return "phase-yellow";
      case "Error":
        return "phase-red";
      default:
        return "phase-neutral";
    }
  }

  function fmtTime(iso) {
    if (!iso) {
      return "–";
    }
    var d = new Date(iso);
    if (isNaN(d.getTime())) {
      return "–";
    }
    return d.toLocaleString();
  }

  function textOrDash(v) {
    if (v === null || v === undefined || v === "") {
      return "–";
    }
    return v;
  }

  function td(value) {
    var cell = document.createElement("td");
    cell.textContent = textOrDash(value);
    return cell;
  }

  function tr(cells) {
    var row = document.createElement("tr");
    for (var i = 0; i < cells.length; i++) {
      row.appendChild(cells[i]);
    }
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
    if (!banner) {
      return;
    }
    if (!message) {
      banner.classList.add("hidden");
      banner.textContent = "";
      return;
    }
    banner.textContent = message;
    banner.classList.remove("hidden");
  }

  function setRefreshIndicator(ok) {
    var indicator = qs("refresh-indicator");
    if (!indicator) {
      return;
    }
    indicator.textContent = ok
      ? "updated " + new Date().toLocaleTimeString()
      : "refresh failed, retrying…";
  }

  function fetchJSON(url) {
    return fetch(url, { cache: "no-store" }).then(function (resp) {
      if (!resp.ok) {
        return resp
          .json()
          .catch(function () {
            return {};
          })
          .then(function (body) {
            var suffix = body && body.error ? ": " + body.error : "";
            throw new Error("HTTP " + resp.status + suffix);
          });
      }
      return resp.json();
    });
  }

  // ---------------- Dashboard page ----------------

  function renderDashboard(clusters) {
    var tbody = qs("clusters-tbody");
    var empty = qs("empty-state");
    tbody.innerHTML = "";

    if (!clusters || clusters.length === 0) {
      empty.classList.remove("hidden");
      return;
    }
    empty.classList.add("hidden");

    clusters.forEach(function (c) {
      var nameCell = document.createElement("td");
      var link = document.createElement("a");
      link.href = "/clusters/" + encodeURIComponent(c.namespace) + "/" + encodeURIComponent(c.name);
      link.textContent = c.name;
      nameCell.appendChild(link);

      var phaseCell = document.createElement("td");
      var badge = document.createElement("span");
      badge.className = "badge " + phaseClass(c.phase);
      badge.textContent = c.phase || "Unknown";
      phaseCell.appendChild(badge);

      var readyText = c.readyPods + " / " + c.totalPods + " (want " + c.desiredPods + ")";
      var backupText = c.backupEnabled ? fmtTime(c.backupLastSuccess) : "disabled";

      tbody.appendChild(
        tr([nameCell, td(c.namespace), phaseCell, td(c.masterPod), td(readyText), td(backupText), td(c.age)])
      );
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

  // ---------------- Detail page ----------------

  function pathParts() {
    // Route is GET /clusters/{namespace}/{name}
    var parts = window.location.pathname.split("/").filter(Boolean);
    return {
      namespace: decodeURIComponent(parts[1] || ""),
      name: decodeURIComponent(parts[2] || ""),
    };
  }

  function renderKV(dl, pairs) {
    dl.innerHTML = "";
    pairs.forEach(function (pair) {
      var dt = document.createElement("dt");
      dt.textContent = pair[0];
      var dd = document.createElement("dd");
      dd.textContent = textOrDash(pair[1]);
      dl.appendChild(dt);
      dl.appendChild(dd);
    });
  }

  function renderDetail(d) {
    document.title = "KiviDB - " + d.name;
    qs("cluster-title").textContent = d.namespace + " / " + d.name;

    var badge = qs("phase-badge");
    badge.textContent = d.phase || "Unknown";
    badge.className = "badge " + phaseClass(d.phase);

    var replicaCount = Math.max((d.desiredPods || 0) - 1, 0);
    renderKV(qs("spec-summary"), [
      ["Image", d.image],
      ["Agent image", d.agentImage],
      ["Port", d.port],
      ["Desired pods", d.desiredPods + " (1 master + " + replicaCount + " replica" + (replicaCount === 1 ? "" : "s") + ")"],
      ["Storage size", d.storageSize],
      ["Storage class", d.storageClassName],
      ["Master service type", d.masterServiceType],
      ["Replica service type", d.replicaServiceType],
    ]);

    var statusPairs = [
      ["Phase", d.phase],
      ["Master pod", d.masterPod],
      ["Pods ready", d.readyPods + " / " + d.totalPods],
      ["Observed generation", d.observedGeneration],
      ["Last failover", fmtTime(d.lastFailoverTime)],
      ["Age", d.age],
    ];
    if (d.statefulSet) {
      statusPairs.push([
        "StatefulSet ready/current/updated",
        d.statefulSet.readyReplicas + " / " + d.statefulSet.currentReplicas + " / " + d.statefulSet.updatedReplicas,
      ]);
    }
    renderKV(qs("status-summary"), statusPairs);

    if (d.backupEnabled) {
      renderKV(qs("backup-summary"), [
        ["Enabled", "yes"],
        ["Schedule", d.backupSchedule],
        ["Retention", d.backupRetention],
        ["Last success", fmtTime(d.backupLastSuccess)],
        ["Last error", d.backupLastError],
        ["CronJob suspended", d.cronJob ? (d.cronJob.suspended ? "yes" : "no") : undefined],
        ["CronJob last schedule", d.cronJob ? fmtTime(d.cronJob.lastScheduleTime) : undefined],
        ["CronJob last success", d.cronJob ? fmtTime(d.cronJob.lastSuccessfulTime) : undefined],
      ]);
    } else {
      renderKV(qs("backup-summary"), [["Enabled", "no"]]);
    }

    var servicesBody = qs("services-table").querySelector("tbody");
    servicesBody.innerHTML = "";
    var services = d.services || [];
    if (services.length === 0) {
      servicesBody.appendChild(emptyRow(5, "No Service objects found yet."));
    } else {
      services.forEach(function (s) {
        servicesBody.appendChild(
          tr([td(s.name), td(s.type), td(s.clusterIP), td(s.externalIP), td((s.ports || []).join(", "))])
        );
      });
    }

    var podsBody = qs("pods-table").querySelector("tbody");
    podsBody.innerHTML = "";
    var pods = d.pods || [];
    if (pods.length === 0) {
      podsBody.appendChild(emptyRow(7, "No pods reported yet."));
    } else {
      pods.forEach(function (p) {
        podsBody.appendChild(
          tr([
            td(p.name),
            td(p.role),
            td(p.ready ? "yes" : "no"),
            td(p.phase),
            td(p.replicationOffset),
            td(p.nodeName),
            td(p.restartCount),
          ])
        );
      });
    }

    var condBody = qs("conditions-table").querySelector("tbody");
    condBody.innerHTML = "";
    var conditions = d.conditions || [];
    if (conditions.length === 0) {
      condBody.appendChild(emptyRow(4, "No conditions reported yet."));
    } else {
      conditions.forEach(function (c) {
        condBody.appendChild(tr([td(c.type), td(c.status), td(c.reason), td(c.message)]));
      });
    }

    var eventsBody = qs("events-table").querySelector("tbody");
    eventsBody.innerHTML = "";
    var events = d.events || [];
    if (events.length === 0) {
      eventsBody.appendChild(emptyRow(5, "No recent events."));
    } else {
      events.forEach(function (e) {
        eventsBody.appendChild(tr([td(fmtTime(e.lastTimestamp)), td(e.type), td(e.reason), td(e.count), td(e.message)]));
      });
    }
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
  }

  // ---------------- Bootstrap ----------------

  document.addEventListener("DOMContentLoaded", function () {
    var page = document.body.dataset.page;
    if (page === "dashboard") {
      refreshDashboard();
      setInterval(refreshDashboard, REFRESH_MS);
    } else if (page === "detail") {
      refreshDetail();
      setInterval(refreshDetail, REFRESH_MS);
    }
  });
})();
