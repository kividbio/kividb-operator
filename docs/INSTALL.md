# Installing kividb-operator

Two supported paths: **Helm** (recommended — also installs the read-only
GUI) or **kustomize/kubectl** (no Helm dependency, GUI installed
separately). Both install into a dedicated namespace,
`kividb-operator-system`, by convention.

## Prerequisites

- Kubernetes 1.27+ (uses `batch/v1` CronJob and structural CRD defaults;
  should work on considerably older versions too, but that's the floor
  this was designed against).
- `kubectl` matching your cluster's minor version or newer.
- Helm 3.9+ if using the Helm path (needed for `crds/` directory support
  and OCI chart references).
- For scheduled backups: an S3-compatible bucket that already exists (the
  operator never creates buckets) and its credentials as a Kubernetes
  Secret — see [BACKUP_RESTORE.md](BACKUP_RESTORE.md).
- For `spec.monitoring.serviceMonitor: true` on any `KividbCluster`, or
  `metrics.serviceMonitor.enabled: true` on the chart: the Prometheus
  Operator CRDs (`monitoring.coreos.com`) must already be installed.

## Option A: Helm

```bash
helm install kividb-operator charts/kividb-operator \
  -n kividb-operator-system --create-namespace
```

Common overrides (see `charts/kividb-operator/values.yaml` for the full,
commented list):

```bash
helm install kividb-operator charts/kividb-operator \
  -n kividb-operator-system --create-namespace \
  --set manager.image.tag=0.1.0 \
  --set gui.enabled=false \
  --set metrics.serviceMonitor.enabled=true
```

Verify:

```bash
kubectl get pods -n kividb-operator-system
kubectl get crd kividbclusters.kividb.io
```

### Upgrading

```bash
helm upgrade kividb-operator charts/kividb-operator -n kividb-operator-system
```

**Helm never touches CRDs on upgrade** — anything under
`charts/kividb-operator/crds/` is installed once, on the very first
`helm install`, and ignored by every subsequent `helm upgrade` (this is a
deliberate Helm 3 design choice, not a bug). If a release changes the CRD
schema, its `CHANGELOG.md` entry will say so; apply the new CRD manually
first:

```bash
kubectl apply -f charts/kividb-operator/crds/kividb.io_kividbclusters.yaml
helm upgrade kividb-operator charts/kividb-operator -n kividb-operator-system
```

### Uninstalling

```bash
helm uninstall kividb-operator -n kividb-operator-system
```

This does **not** delete the CRD (Helm's `crds/` convention again) or any
`KividbCluster` objects/their StatefulSets/PVCs — the operator simply
stops reconciling them. To remove everything, including all data:

```bash
kubectl delete kividbcluster --all -A     # deletes every managed StatefulSet/Service/etc. via ownerReferences
kubectl delete crd kividbclusters.kividb.io
```

**`kubectl delete crd` deletes every `KividbCluster` object across every
namespace in the cluster, cascading to their PVCs' reclaim policy.**
Double-check you mean *every* cluster in the whole Kubernetes cluster, not
just one, before running it.

## Option B: kustomize / kubectl

Installs the CRD, RBAC, and manager Deployment (no GUI — deploy it
separately from `config/gui/`, see [GUI.md](GUI.md)):

```bash
kubectl apply -k config/default
```

This is equivalent to (and internally composed from):

```bash
kubectl apply -k config/crd       # CustomResourceDefinition
kubectl apply -k config/rbac      # ServiceAccount, ClusterRole(Binding), leader-election Role(Binding)
kubectl apply -k config/manager   # Deployment
```

Or, without kustomize at all, apply the pieces directly:

```bash
kubectl apply -f config/crd/bases/kividb.io_kividbclusters.yaml
kubectl apply -f config/rbac/
kubectl apply -f config/manager/manager.yaml
```

### Upgrading / uninstalling (kustomize path)

```bash
kubectl apply -k config/default      # upgrade: re-applies everything, including the CRD this time
kubectl delete -k config/default     # uninstall: removes CRD, RBAC and the manager Deployment
```

Unlike the Helm path, `kubectl apply -k config/default` **does** update
the CRD on every apply, since there's no Helm `crds/`-style special
casing here — this is one real behavioral difference between the two
install paths, not just a packaging difference.

## Verifying the install

```bash
kubectl get deploy -n kividb-operator-system
kubectl logs -n kividb-operator-system deploy/kividb-operator-manager -f
kubectl apply -f config/samples/kividb.io_v1alpha1_kividbcluster_minimal.yaml
kubectl get kividbcluster -w
```

Expect `status.phase` to move `Pending` → `Provisioning` → `Running`
within roughly a minute for the minimal sample (mostly bounded by image
pull time and PVC provisioning). See
[TROUBLESHOOTING.md](TROUBLESHOOTING.md) if it doesn't.
