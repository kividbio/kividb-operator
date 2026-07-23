<p align="center">
  <a href="https://kividb.io">
    <img  src="assets/banner.jpeg"
      alt="KiviDB">
  </a>
</p>

[![Artifact Hub](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/kividb-operator)](https://artifacthub.io/packages/search?repo=kividb-operator)
[![Release](https://github.com/kividbio/kividb-operator/actions/workflows/release.yaml/badge.svg)](https://github.com/kividbio/kividb-operator/actions/workflows/release.yaml)

A Kubernetes operator for [kividb](https://kividb.io), a
Redis-RESP-compatible in-memory store written in Rust. It manages a
`KividbCluster` custom resource describing a single-master, N-replica
kividb cluster, and handles:

- **Automatic failover** — an unhealthy master is detected and replaced by
  promoting the most caught-up replica, with pod labels (not Service
  specs) moved to reflect the new topology.
- **Separate master/replica Services**, each independently configurable
  as `ClusterIP`, `NodePort`, or `LoadBalancer` (with static IP /
  annotations / source-range support).
- **ACL user management** and legacy `requirepass`, rendered into
  kividb's Redis-compatible ACL file from Kubernetes Secrets.
- **Scheduled snapshot backups** to any S3-compatible object storage
  (AWS S3, MinIO, R2, B2, Ceph RGW, ...), via a native Kubernetes
  `CronJob` and per-pod retention pruning.
- **Standard scheduling controls** — `resources`, `tolerations`,
  `nodeSelector`, `affinity` — plus Prometheus metrics and a read-only
  web dashboard.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for how it all fits
together, including a sequence diagram of what actually happens during a
failover and a backup.

## How it works, in one picture

Every kividb pod runs two containers — `kividb` (the database itself) and
`agent` (a small Go sidecar that speaks HTTP to the controller and RESP to
kividb — kividb has no HTTP API of its own) — plus a third, optional
`redis-exporter` container (`oliver006/redis_exporter`) when
`spec.monitoring.enabled: true`, for Prometheus metrics in the standard
Redis-ecosystem format. The controller never talks RESP directly; it only
calls the agent.

```
KividbCluster (CRD)
        │
        ▼
  operator controller ──HTTP──▶ agent sidecar ──RESP──▶ kividb
        │                         (per pod)
        ├─▶ StatefulSet (pods, role tracked via label kividb.io/role)
        ├─▶ Service <name>-master     (selects role=master)
        ├─▶ Service <name>-replicas   (selects role=replica)
        ├─▶ ConfigMap <name>-config   (kividb.conf)
        ├─▶ Secret <name>-auth        (ACL file)
        └─▶ CronJob <name>-backup     (triggers agent's /backup over HTTP)
```

## Quickstart

**Prerequisites**: a Kubernetes cluster (1.27+) and `kubectl`. Helm 3 is
recommended for install; a `kubectl apply -k` (kustomize) path is also
available. See [docs/INSTALL.md](docs/INSTALL.md) for both, plus upgrade
and uninstall instructions.

```bash
# 1. Install the operator (CRD + controller + read-only GUI) into its own namespace
helm install kividb-operator charts/kividb-operator \
  -n kividb-operator-system --create-namespace

# 2. Apply a minimal cluster
kubectl apply -f config/samples/kividb.io_v1alpha1_kividbcluster_minimal.yaml

# 3. Watch it come up
kubectl get kividbcluster -w
kubectl get pods -l kividb.io/cluster=kividb-sample

# 4. Connect (from inside the cluster, or via port-forward)
kubectl port-forward svc/kividb-sample-master 6380:6380
redis-cli -p 6380 ping
```

For a full-featured example — replicas, custom `kividbConfig` directives,
ACL users, tolerations/nodeSelector, S3-backed backups, a LoadBalancer
master Service, and monitoring — see
[`config/samples/kividb.io_v1alpha1_kividbcluster_full.yaml`](config/samples/kividb.io_v1alpha1_kividbcluster_full.yaml)
(paired with
[`..._full-secrets.yaml`](config/samples/kividb.io_v1alpha1_kividbcluster_full-secrets.yaml)
for the Secrets it references) and the full field reference in
[docs/CONFIGURATION.md](docs/CONFIGURATION.md).

## The web GUI

A read-only dashboard (`cmd/gui`) ships alongside the operator
(`gui.enabled: true` by default in the Helm chart) listing every
`KividbCluster`, its phase, master/replica pods with readiness and
replication offset, and recent events. It never has RBAC access to
Secrets. See [docs/GUI.md](docs/GUI.md).

```bash
kubectl port-forward -n kividb-operator-system svc/kividb-operator-gui 8090:8090
# open http://localhost:8090
```

## Documentation

| Doc | Covers |
|---|---|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Design decisions, the agent sidecar, failover and backup sequence diagrams |
| [docs/INSTALL.md](docs/INSTALL.md) | Helm and kustomize install, upgrade, uninstall |
| [docs/CONFIGURATION.md](docs/CONFIGURATION.md) | Full `KividbCluster` CRD field reference |
| [docs/BACKUP_RESTORE.md](docs/BACKUP_RESTORE.md) | Configuring S3 backups, triggering one on demand, manual restore procedure |
| [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) | Symptom → cause → fix for common issues |
| [docs/GUI.md](docs/GUI.md) | Running and deploying the dashboard |
| [docs/RELEASING.md](docs/RELEASING.md) | Versioning and the release pipeline |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Local dev setup, code layout, PR checklist |

## Status

This project is at `v0.1.0` — see [CHANGELOG.md](CHANGELOG.md). The CRD
API version is `kividb.io/v1alpha1`: expect it to evolve. There is no
network access in every environment this repo has been built in so far,
so treat the first CI run against a given commit as the first real
`go build`/`go test` correctness check (see
[docs/RELEASING.md](docs/RELEASING.md) for what CI verifies).

## License

[Apache License 2.0](LICENSE).
