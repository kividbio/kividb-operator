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
  --set manager.image.tag=0.3.0 \
  --set gui.enabled=false \
  --set metrics.serviceMonitor.enabled=true
```

Verify:

```bash
kubectl get pods -n kividb-operator-system
kubectl get crds -l app.kubernetes.io/name=kividb-operator
# or explicitly, all six kinds:
kubectl get crd kividbclusters.kividb.io kividbconfigs.kividb.io \
  kividbaclconfigs.kividb.io kividbsnapshotconfigs.kividb.io kividbsnapshots.kividb.io \
  kividbdbops.kividb.io
```

### Watching one namespace

By default the operator watches every namespace. To limit the manager and
the GUI to one, set `watchNamespace` to a namespace that already exists:

```bash
helm install kividb-operator charts/kividb-operator \
  -n kividb-operator-system --create-namespace \
  --set watchNamespace=team-a
```

Both get `WATCH_NAMESPACE=team-a`, and their RBAC is rendered as Roles and
RoleBindings in `team-a` instead of ClusterRoles and ClusterRoleBindings.
The CRDs stay cluster-scoped. If `team-a` is not the release namespace, a
small Role there lets the manager read and create its agent token Secret.
The GUI keeps one cluster-wide permission, `get`/`list` on StorageClasses,
for its create form: StorageClasses are cluster-scoped, so a Role cannot
grant them.

Each release names its roles and bindings after itself, so several
releases can run side by side, one per team namespace, each with its own
`watchNamespace`; uninstalling one leaves the others' permissions alone.

KividbClusters in other namespaces are ignored. If you restrict an existing
install, the clusters outside `team-a` keep running but are no longer
reconciled: no failover, no backups, no spec changes. Deleting one still
works (the operator sets no finalizers on clusters), and its owned objects
are removed by garbage collection. A `KividbConfig`, `KividbAclConfig` or
`KividbSnapshotConfig` there that was in use when the namespace stopped
being watched keeps its `kividb.io/in-use` finalizer, and a delete of it
stays Terminating: clear the finalizer by hand, as in
[Uninstalling](#uninstalling).

**Deleting a namespace the operator runs in.** With `watchNamespace` set to
the release's own namespace, `kubectl delete namespace` removes the
operator along with the clusters, and it can go first. A config it had not
yet released then keeps its `kividb.io/in-use` finalizer, and the namespace
stays Terminating. Delete the clusters before the namespace, or release a
config by hand:

```bash
kubectl patch kividbconfig <name> -n <namespace> --type=json \
  -p='[{"op":"remove","path":"/metadata/finalizers"}]'
```

(the same for `kividbaclconfig` and `kividbsnapshotconfig`; the loop in
[Uninstalling](#uninstalling) does every one).

Without Helm, set `WATCH_NAMESPACE` on the manager to one namespace or a
comma-separated list (OLM does this for an OperatorGroup's target
namespaces). The GUI takes one namespace only.

### Upgrading

```bash
helm upgrade kividb-operator charts/kividb-operator -n kividb-operator-system
```

**Helm never touches CRDs on upgrade** — anything under
`charts/kividb-operator/crds/` is installed once, on the very first
`helm install`, and ignored by every subsequent `helm upgrade` (this is a
deliberate Helm 3 design choice, not a bug). A release can both change
existing CRDs and add new ones, so apply the whole directory first, every
time:

```bash
kubectl apply -f charts/kividb-operator/crds/
helm upgrade kividb-operator charts/kividb-operator -n kividb-operator-system
```

If you skip this when upgrading to a release that adds a kind (0.4.0 adds
`KividbDbOps`), the operator keeps managing clusters but logs that the
kind is not installed and ignores objects of it; apply the CRDs and
restart the operator Deployment to enable it.

**An upgrade can restart every cluster.** A release that changes the
default agent image (every release does) or the default engine image rolls
each `KividbCluster` that leaves `spec.agentImage` or `spec.image` unset,
one pod at a time. A master with replicas hands its role to an in-sync
replica first. A master with none (`replicas: 0`, in a replicated cluster
or in each shard) is restarted in place: the cluster, or that shard's
slots, are unavailable until it is back, and without the append-only file
it keeps only what its shutdown snapshot saved. Before upgrading, give it
a replica, or keep the append-only file on, or pin both images to put the
restart off. The CHANGELOG's "Read before upgrading" says what a given
release changes.

### Going back to 0.7.1

0.7.1 does not know the `kividb.io/in-use` finalizer that later releases
put on configs in use, and never removes it: after a rollback, deleting
such a `KividbConfig`, `KividbAclConfig` or `KividbSnapshotConfig` leaves it
Terminating. Remove the finalizer from every config, before or after
rolling back, with the loop in [Uninstalling](#uninstalling).

### Uninstalling

```bash
helm uninstall kividb-operator -n kividb-operator-system
```

This does **not** delete any of the six CRDs (Helm's `crds/` convention
again) or any `KividbCluster`/`KividbConfig`/`KividbAclConfig`/
`KividbSnapshotConfig`/`KividbSnapshot`/`KividbDbOps` objects/their
StatefulSets/PVCs — the operator simply stops reconciling them. To remove
everything, including all data:

```bash
kubectl delete kividbcluster --all -A     # deletes every managed StatefulSet/Service/etc. via ownerReferences
kubectl delete crd kividbclusters.kividb.io kividbconfigs.kividb.io \
  kividbaclconfigs.kividb.io kividbsnapshotconfigs.kividb.io kividbsnapshots.kividb.io \
  kividbdbops.kividb.io
```

A `KividbConfig`, `KividbAclConfig` or `KividbSnapshotConfig` that a
cluster references carries the `kividb.io/in-use` finalizer, which the
operator removes once nothing references it. Delete the clusters while the
operator is still running and it clears them itself. If you uninstalled the
operator first, a config that still had the finalizer stays Terminating
(and so does `kubectl delete crd` for its kind, and a namespace delete).
Clear the finalizer by hand:

```bash
for kind in kividbconfig kividbaclconfig kividbsnapshotconfig; do
  kubectl get "$kind" -A -o jsonpath='{range .items[*]}{.metadata.namespace} {.metadata.name}{"\n"}{end}' |
  while read -r ns name; do
    kubectl patch "$kind" "$name" -n "$ns" --type=json \
      -p='[{"op":"remove","path":"/metadata/finalizers"}]' 2>/dev/null || true
  done
done
```

**`kubectl delete crd` deletes every object of that kind across every
namespace in the cluster** — for `kividbclusters.kividb.io` specifically,
that cascades to their PVCs' reclaim policy. Double-check you mean
*every* cluster in the whole Kubernetes cluster, not just one, before
running it.

## Option B: kustomize / kubectl

Installs the CRD, RBAC, and manager Deployment (no GUI — deploy it
separately with `kubectl apply -k config/gui`, see [GUI.md](GUI.md)).
`config/default` does not create its namespace, so create it first
(`make deploy` does both):

```bash
kubectl create namespace kividb-operator-system --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -k config/default
```

This is equivalent to (and internally composed from):

```bash
kubectl apply -k config/crd       # CustomResourceDefinition
kubectl apply -k config/rbac      # ServiceAccount, ClusterRole(Binding), leader-election Role(Binding)
kubectl apply -k config/manager   # Deployment
```

Or, without kustomize at all, apply the pieces directly. `config/rbac/`
also holds a `kustomization.yaml`, which `kubectl apply -f` rejects, so its
files are named one by one. None of these manifests creates the namespace,
so the first line does:

```bash
kubectl create namespace kividb-operator-system --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f config/crd/bases/
kubectl apply -f config/rbac/service_account.yaml \
  -f config/rbac/role.yaml -f config/rbac/role_binding.yaml \
  -f config/rbac/leader_election_role.yaml \
  -f config/rbac/leader_election_role_binding.yaml
kubectl apply -f config/manager/manager.yaml
```

### Upgrading / uninstalling (kustomize path)

```bash
kubectl apply -k config/default      # upgrade: re-applies everything, including the CRD this time
kubectl delete -k config/default     # uninstall: removes CRD, RBAC and the manager Deployment
```

`kubectl delete -k config/default` removes the manager before the CRDs, so
delete your clusters first, or clear the configs' `kividb.io/in-use`
finalizer as in [Uninstalling](#uninstalling); otherwise the deletion of a
config CRD waits on the configs that still hold it.

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
