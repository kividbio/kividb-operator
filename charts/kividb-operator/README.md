# kividb-operator

A Kubernetes operator for [kividb](https://kividb.io), a
Redis-RESP-compatible in-memory store. This chart installs the operator
(the `KividbCluster` CRD, its controller, RBAC, and an optional read-only
web dashboard) — it does **not** install a database cluster by itself.
Once installed, you create `KividbCluster` custom resources to describe
the clusters you actually want; see the samples linked below.

## What you get

- The `KividbCluster` CRD (`kividb.io/v1alpha1`) and its controller,
  managing single-master/N-replica StatefulSets with automatic failover
  (a replica is promoted and pods relabeled — no Service changes needed),
  ACL/`requirepass` auth, separate master/replica Services
  (`ClusterIP`/`NodePort`/`LoadBalancer`), and scheduled S3-compatible
  backups via native `CronJob`s.
- A per-pod agent sidecar that bridges HTTP (from the controller) to RESP
  (to kividb), since kividb has no HTTP API of its own.
- An optional third `redis-exporter` sidecar (`oliver006/redis_exporter`)
  per `KividbCluster` pod, for Prometheus metrics in the standard
  Redis-ecosystem format, plus an optional cluster-wide `ServiceMonitor`.
- An optional read-only web GUI listing every `KividbCluster`'s status,
  running under its own narrow, Secret-free RBAC.

Full design writeup: [docs/ARCHITECTURE.md](https://github.com/kividbio/kividb-operator/blob/main/docs/ARCHITECTURE.md).

## Installing

```bash
helm install kividb-operator oci://quay.io/kividbio/kividb-operator-chart \
  --version 0.1.0 \
  -n kividb-operator-system --create-namespace
```

(Pass whichever version you're installing to `--version`; omit it to get
the latest. See [docs/INSTALL.md](https://github.com/kividbio/kividb-operator/blob/main/docs/INSTALL.md)
for the full install/upgrade/uninstall walkthrough, including the
kustomize-only alternative and why Helm never auto-upgrades the CRD.)

Then apply a `KividbCluster`:

```bash
kubectl apply -f https://raw.githubusercontent.com/kividbio/kividb-operator/main/config/samples/kividb.io_v1alpha1_kividbcluster_minimal.yaml
kubectl get kividbcluster -w
```

## Key values

See [`values.yaml`](https://github.com/kividbio/kividb-operator/blob/main/charts/kividb-operator/values.yaml)
for the complete, commented list. The most commonly overridden:

| Key | Default | Purpose |
|---|---|---|
| `manager.image.tag` | chart's `appVersion` | Operator manager image tag |
| `gui.enabled` | `true` | Install the read-only dashboard |
| `gui.service.type` | `ClusterIP` | How to expose the GUI |
| `crds.install` | `true` | Install the CRD on first install (Helm convention: never auto-upgraded afterward — see `docs/INSTALL.md`) |
| `metrics.serviceMonitor.enabled` | `false` | Cluster-wide `ServiceMonitor` for every `KividbCluster`'s agent + `redis-exporter` metrics |
| `watchNamespace` | `""` (all namespaces) | Restrict the manager to one namespace |

Per-`KividbCluster` settings (replicas, storage, ACL users, backup
schedule, the `redis-exporter` sidecar, scheduling constraints, ...) are
**not** chart values — they live on the `KividbCluster` custom resource
itself. See [docs/CONFIGURATION.md](https://github.com/kividbio/kividb-operator/blob/main/docs/CONFIGURATION.md)
for the full field reference.

## Documentation

[README](https://github.com/kividbio/kividb-operator#readme) ·
[Architecture](https://github.com/kividbio/kividb-operator/blob/main/docs/ARCHITECTURE.md) ·
[Install](https://github.com/kividbio/kividb-operator/blob/main/docs/INSTALL.md) ·
[Configuration](https://github.com/kividbio/kividb-operator/blob/main/docs/CONFIGURATION.md) ·
[Backup & Restore](https://github.com/kividbio/kividb-operator/blob/main/docs/BACKUP_RESTORE.md) ·
[Troubleshooting](https://github.com/kividbio/kividb-operator/blob/main/docs/TROUBLESHOOTING.md)

## License

[Apache License 2.0](https://github.com/kividbio/kividb-operator/blob/main/LICENSE).
