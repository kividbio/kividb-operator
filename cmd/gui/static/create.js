// New cluster: a form, previewed as the API server's dry run of it.
(function () {
  "use strict";
  var KV = window.KV;
  var el = KV.el;

  var options = null;
  var variant = "standard";
  var imageTouched = false;
  var previewTimer = null;
  var previewSeq = 0;
  var NAME_RE = /^[a-z0-9]([-a-z0-9]*[a-z0-9])?$/;

  function val(id) {
    return KV.qs(id).value.trim();
  }

  function variantImage(v) {
    var list = (options && options.variants) || [];
    for (var i = 0; i < list.length; i++) if (list[i].name === v) return list[i].image;
    var base = KV.session.defaultImage || "docker.io/kividb/kividb";
    return v === "standard" ? base : base + "-" + v;
  }

  function request() {
    return {
      namespace: val("f-namespace"),
      name: val("f-name"),
      replicas: Number(KV.qs("f-replicas").value),
      variant: variant,
      image: val("f-image"),
      storageSize: val("f-storage"),
      storageClassName: KV.qs("f-storageclass").value,
      memory: val("f-memory"),
      cpu: val("f-cpu"),
      snapshotConfig: KV.qs("f-snapshot").value,
      masterServiceType: KV.qs("f-service").value,
      monitoring: KV.qs("f-monitoring").checked,
    };
  }

  function validateName() {
    var name = val("f-name");
    var msg = "";
    if (name && !NAME_RE.test(name)) msg = "Lowercase letters, digits and dashes; it must start and end with a letter or digit.";
    if (name.length > 40) msg = "At most 40 characters.";
    var e = KV.qs("e-name");
    e.textContent = msg;
    e.classList.toggle("hidden", !msg);
    KV.qs("f-name").classList.toggle("invalid", !!msg);
    return !msg && !!name;
  }

  function updateReplicasHint() {
    var n = Number(KV.qs("f-replicas").value) || 0;
    KV.qs("h-replicas").textContent =
      "1 master + " + n + (n === 1 ? " replica" : " replicas") + ": " + (n + 1) + " pods." + (n === 0 ? " No failover without a replica." : "");
  }

  function setVariant(v) {
    variant = v;
    document.querySelectorAll("#variant-cards .option-card").forEach(function (c) {
      c.classList.toggle("active", c.dataset.variant === v);
    });
    if (!imageTouched) KV.qs("f-image").value = variantImage(v);
    schedulePreview();
  }

  function schedulePreview() {
    clearTimeout(previewTimer);
    previewTimer = setTimeout(preview, 350);
  }

  // The manifest as the API server would store it (defaults filled in), or
  // what is wrong with the form.
  function preview() {
    var seq = ++previewSeq;
    var req = request();
    var nameOK = validateName();
    if (!req.name) req.name = "my-cluster";
    var pre = KV.qs("yaml");
    if (!nameOK && val("f-name")) return;
    KV.sendJSON("POST", "/api/clusters?dryRun=1", req)
      .then(function (j) {
        if (seq !== previewSeq) return;
        pre.textContent = j.yaml;
        pre.classList.remove("err");
        KV.qs("create-error").classList.add("hidden");
        KV.qs("preview-note").textContent = "Checked by the API server as you type (a dry run: nothing is created).";
      })
      .catch(function (e) {
        if (seq !== previewSeq) return;
        if (e.status === 403 && !KV.session.writable) {
          KV.qs("preview-note").textContent = "Read-only GUI: the server can't check it, so this is the form as typed.";
          pre.textContent = localYAML(req);
          return;
        }
        var box = KV.qs("create-error");
        box.textContent = e.message;
        box.classList.remove("hidden");
      });
  }

  // A plain rendering of the request, for a read-only GUI that may not dry-run.
  function localYAML(r) {
    var lines = ["apiVersion: kividb.io/v1alpha1", "kind: KividbCluster", "metadata:", "  name: " + r.name, "  namespace: " + r.namespace, "spec:"];
    lines.push("  image: " + r.image);
    lines.push("  variant: " + r.variant);
    lines.push("  replicas: " + r.replicas);
    lines.push("  storage:");
    lines.push("    size: " + (r.storageSize || "1Gi"));
    if (r.storageClassName) lines.push("    storageClassName: " + r.storageClassName);
    if (r.memory || r.cpu) {
      lines.push("  resources:");
      if (r.memory) lines.push("    limits:", "      memory: " + r.memory);
      lines.push("    requests:");
      if (r.cpu) lines.push("      cpu: " + r.cpu);
      if (r.memory) lines.push("      memory: " + r.memory);
    }
    if (r.snapshotConfig) lines.push("  snapshotConfigRef:", "    name: " + r.snapshotConfig);
    if (r.masterServiceType && r.masterServiceType !== "ClusterIP") lines.push("  services:", "    master:", "      type: " + r.masterServiceType);
    if (r.monitoring) lines.push("  monitoring:", "    enabled: true");
    return lines.join("\n") + "\n";
  }

  function loadOptions() {
    var ns = val("f-namespace") || "default";
    return KV.fetchJSON("/api/create-options?namespace=" + encodeURIComponent(ns))
      .then(function (o) {
        options = o;
        var sc = KV.qs("f-storageclass");
        var cur = sc.value;
        KV.clear(sc);
        sc.appendChild(el("option", { value: "", text: "cluster default" }));
        (o.storageClasses || []).forEach(function (c) {
          sc.appendChild(el("option", { value: c.name, text: c.name + (c.default ? " (default)" : "") }));
        });
        sc.value = cur;
        var snap = KV.qs("f-snapshot");
        var curSnap = snap.value;
        KV.clear(snap);
        snap.appendChild(el("option", { value: "", text: "off" }));
        (o.snapshotConfigs || []).forEach(function (n) {
          snap.appendChild(el("option", { value: n, text: n }));
        });
        snap.value = (o.snapshotConfigs || []).indexOf(curSnap) >= 0 ? curSnap : "";
        if (!imageTouched) KV.qs("f-image").value = variantImage(variant);
      })
      .catch(function () {});
  }

  function create(ev) {
    ev.preventDefault();
    if (!validateName()) {
      KV.qs("f-name").focus();
      if (!val("f-name")) {
        KV.qs("e-name").textContent = "Give the cluster a name.";
        KV.qs("e-name").classList.remove("hidden");
      }
      return;
    }
    var btn = KV.qs("btn-create");
    btn.disabled = true;
    btn.textContent = "Creating…";
    var req = request();
    KV.sendJSON("POST", "/api/clusters", req)
      .then(function (j) {
        KV.toast("Created " + j.namespace + "/" + j.name);
        window.location.href = "/clusters/" + encodeURIComponent(j.namespace) + "/" + encodeURIComponent(j.name);
      })
      .catch(function (e) {
        var box = KV.qs("create-error");
        box.textContent = e.message;
        box.classList.remove("hidden");
        btn.disabled = !KV.session.writable;
        btn.textContent = "Create cluster";
      });
  }

  document.addEventListener("DOMContentLoaded", function () {
    document.querySelectorAll("#variant-cards .option-card").forEach(function (c) {
      c.addEventListener("click", function () {
        setVariant(c.dataset.variant);
      });
      c.addEventListener("keydown", function (ev) {
        if (ev.key === "Enter" || ev.key === " ") {
          ev.preventDefault();
          setVariant(c.dataset.variant);
        }
      });
    });
    KV.qs("f-image").addEventListener("input", function () {
      imageTouched = KV.qs("f-image").value.trim() !== "";
    });
    KV.qs("rep-dec").addEventListener("click", function () {
      var i = KV.qs("f-replicas");
      i.value = Math.max(0, Number(i.value) - 1);
      updateReplicasHint();
      schedulePreview();
    });
    KV.qs("rep-inc").addEventListener("click", function () {
      var i = KV.qs("f-replicas");
      i.value = Math.min(9, Number(i.value) + 1);
      updateReplicasHint();
      schedulePreview();
    });
    KV.qs("f-replicas").addEventListener("input", updateReplicasHint);
    KV.qs("create-form").addEventListener("input", schedulePreview);
    KV.qs("create-form").addEventListener("change", schedulePreview);
    KV.qs("f-namespace").addEventListener("change", function () {
      loadOptions().then(schedulePreview);
    });
    KV.qs("create-form").addEventListener("submit", create);
    KV.qs("btn-copy").addEventListener("click", function () {
      var text = KV.qs("yaml").textContent;
      (navigator.clipboard ? navigator.clipboard.writeText(text) : Promise.reject())
        .then(function () {
          KV.toast("Manifest copied", "info");
        })
        .catch(function () {
          KV.toast("Select the manifest and copy it by hand.", "info");
        });
    });

    KV.fetchJSON("/api/clusters")
      .then(function (list) {
        var seen = {};
        var dl = KV.qs("ns-list");
        (list || []).forEach(function (c) {
          if (seen[c.namespace]) return;
          seen[c.namespace] = true;
          dl.appendChild(el("option", { value: c.namespace }));
        });
      })
      .catch(function () {});

    KV.loadSession().then(function (s) {
      if (s.watchNamespace) {
        KV.qs("f-namespace").value = s.watchNamespace;
        KV.qs("f-namespace").readOnly = true;
      }
      KV.qs("readonly-banner").classList.toggle("hidden", !!s.writable);
      KV.applyWriteMode();
      updateReplicasHint();
      loadOptions().then(preview);
    });
    KV.qs("f-name").focus();
  });
})();
