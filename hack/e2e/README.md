# Minikube e2e / compat harness

End-to-end suites for [kividb-operator](../../README.md) against a local
minikube cluster. Layout follows the same multi-CRD split as StackGres
(`KividbConfig` / `KividbSnapshotConfig` applied separately from
`KividbCluster`) and the Dragonfly-operator pattern (StatefulSet + sidecar
agent; clients talk to the master Service selected by `kividb.io/role`).

## Prerequisites

| Tool | Notes |
|------|--------|
| `kubectl`, `helm`, `minikube`, `docker` | Cluster + chart install |
| `redis-cli` | PING / SET / GET via port-forward |
| `openssl` | Self-signed TLS Secret for `tls`/`full` variants |
| Optional: `redis-benchmark`, `curl` | Load + metrics scrape |

Start minikube with enough headroom (Prometheus + MinIO + a 3-pod cluster):

```bash
minikube start --cpus=4 --memory=6144 --driver=docker
```

## Required images

| Image | Purpose |
|-------|---------|
| `quay.io/kividbio/kividb-operator:${OPERATOR_TAG}` | Manager |
| `quay.io/kividbio/kividb-operator-agent:${OPERATOR_TAG}` | Sidecar + backup-trigger |
| `quay.io/kividbio/kividb-operator-gui:${OPERATOR_TAG}` | Optional GUI |
| `quay.io/kividbio/kividb:v1.0.3` (+ `-tls`, `-lua`, `-full`) | Engine variants |
| `quay.io/minio/minio`, `quay.io/minio/mc` | In-cluster S3 for snapshots |
| `prometheus-community/kube-prometheus-stack` | Optional monitoring |

Build operator images locally, then load them into minikube:

```bash
make docker-build VERSION=0.3.0-local
# or: docker build -t quay.io/kividbio/kividb-operator:0.3.0-local -f Dockerfile .
LOAD_IMAGES=1 OPERATOR_TAG=0.3.0-local make e2e
```

## Quick start

```bash
# Full suite
./hack/e2e/run-all.sh
# or
make e2e

# Individual steps
make e2e-prereqs
make e2e-deploy
./hack/e2e/03-minio.sh
./hack/e2e/04-compat-variants.sh
```

Results land in `hack/e2e/results/latest.txt` (pass/fail/skip per suite).

## Environment variables

| Variable | Default | Meaning |
|----------|---------|---------|
| `OPERATOR_TAG` | `0.3.0-local` | Manager / agent / GUI image tag |
| `OPERATOR_REGISTRY` | `quay.io/kividbio` | Image registry prefix |
| `KIVIDB_VERSION` | `v1.0.3` | Engine tag prefix (`v1.0.3`, `v1.0.3-tls`, …) |
| `LOAD_IMAGES` | `0` | `1` → `minikube image load` for operator (+ engine in compat) |
| `STRICT_TLS` | `0` | `1` → fail if TLS port is not LISTEN (else warn; ROADMAP) |
| `STORAGE_CLASS` | `standard` | PVC StorageClass (minikube default) |
| `PVC_SIZE` | `1Gi` | Per-pod data volume |
| `OPERATOR_NS` | `kividb-operator-system` | Helm release namespace |
| `E2E_NS` | `e2e-ns` | MinIO namespace |
| `E2E_KIVIDB_NS` | `e2e-kividb` | Test cluster namespace |
| `MONITORING_NS` | `monitoring` | kube-prometheus-stack |
| `STOP_ON_FAIL` | `0` | `1` → abort `run-all.sh` on first failure |

### Skip flags (`run-all.sh`)

| Flag | Skips |
|------|--------|
| `SKIP_DEPLOY=1` | `01-deploy-operator.sh` |
| `SKIP_PROMETHEUS=1` | `02-prometheus.sh` |
| `SKIP_MINIO=1` | `03-minio.sh` |
| `SKIP_COMPAT=1` | `04-compat-variants.sh` |
| `SKIP_FAILOVER=1` | `05-failover-load.sh` |
| `SKIP_SNAPSHOT=1` | `06-snapshot-chaos.sh` |
| `SKIP_MONITOR=1` | `07-monitor-memory.sh` |

Example: re-run failover only against an already-deployed operator:

```bash
SKIP_DEPLOY=1 SKIP_PROMETHEUS=1 SKIP_MINIO=1 SKIP_COMPAT=1 \
  SKIP_SNAPSHOT=1 SKIP_MONITOR=1 ./hack/e2e/run-all.sh
```

## Suite map

| Script | What it covers |
|--------|----------------|
| `00-prereqs.sh` | Tools + minikube running + arch |
| `01-deploy-operator.sh` | Helm install, CRDs; ServiceMonitor enabled when CRD exists |
| `02-prometheus.sh` | Minimal kube-prometheus-stack; then enables chart ServiceMonitor |
| `03-minio.sh` | MinIO Deployment/Service + bucket Job (`minioadmin`/`minioadmin`) |
| `04-compat-variants.sh` | `standard`/`tls`/`lua`/`full` PING + TLS `/proc/net/tcp` + VariantGuidance |
| `05-failover-load.sh` | Load + force-delete master + role election |
| `06-snapshot-chaos.sh` | Minute CronJob, kill source / Job pod, MinIO object check |
| `07-monitor-memory.sh` | Agent `/metrics`, memory/commands under load, optional Prom targets |

## Notes

- **TLS:** ROADMAP records `-tls`/`-full` not opening a TLS listener on
  v1.0.2. Compat re-checks on `KIVIDB_VERSION` and **warns** by default;
  set `STRICT_TLS=1` to fail the suite if LISTEN is still missing.
- **Replication:** After failover, seed-key presence is best-effort — see
  the upstream replication caveat in `docs/ROADMAP.md`.
- **Secrets:** MinIO uses generated local defaults only
  (`minioadmin` / `minioadmin`). Do not commit real cloud credentials.
- **Resources:** Requests are intentionally tiny (`50m`/`64Mi` class) so
  the suite fits a laptop minikube.
