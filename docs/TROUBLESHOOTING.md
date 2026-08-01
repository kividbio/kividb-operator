# Troubleshooting

Start with the cluster's own status — it's usually the fastest path to a
diagnosis:

```bash
kubectl get kividbcluster my-cluster -o yaml
kubectl describe kividbcluster my-cluster   # shows Conditions + recent Events
kubectl get pods -l kividb.io/cluster=my-cluster -o wide --show-labels
kubectl logs deploy/kividb-operator-manager -n kividb-operator-system
```

## Cluster stuck in `Provisioning`

**Cause: pods not yet Ready.** Check pod events for the actual reason:

```bash
kubectl describe pod my-cluster-0
```

Common culprits:
- **PVC pending** — no matching StorageClass, or the provisioner can't
  satisfy `spec.storage.size`. Check `kubectl get pvc -l kividb.io/cluster=my-cluster`.
- **ImagePullBackOff** — `spec.image` or `spec.agentImage` typo'd, or
  missing `imagePullSecrets` for a private registry.
- **Readiness probe failing** — the kividb container's readiness probe
  hits the *agent's* `/readyz` (port 8081), which does a real RESP `PING`.
  `kubectl exec` into the `agent` container and curl it yourself:

  ```bash
  kubectl exec my-cluster-0 -c agent -- wget -qO- http://localhost:8081/readyz
  ```

  If that fails with an auth error, your `KividbAclConfig`'s password
  Secret and the agent's `KIVIDB_AUTH_PASSWORD` env var have drifted —
  see the ACL section below.

**Cause: no ready pod yet to elect as master.** This is expected and
self-heals: until at least one pod is Ready, the controller logs (and
briefly surfaces via the `Ready` condition) `no ready pod available to
elect as master`. It requeues every 5s and resolves itself as soon as the
first pod passes its readiness probe. If this persists for minutes, it's
really the pod-readiness problem above, not an election problem.

## No master elected / `status.masterPod` is empty despite Ready pods

Check whether the operator can actually reach pod agents over the network
(NetworkPolicy misconfiguration is the usual cause here):

```bash
kubectl exec -n kividb-operator-system deploy/kividb-operator-manager -- \
  wget -qO- http://<pod-ip>:8081/status
```

If that hangs/fails from the operator's pod but works from elsewhere, a
`NetworkPolicy` is likely blocking traffic from the operator's namespace
to port 8081 on kividb pods — allow it explicitly.

## Failover keeps happening repeatedly ("flapping")

- Check `status.lastFailoverTime` and `kubectl get events
  --field-selector involvedObject.name=my-cluster` for a timeline.
- If the *new* master also gets marked unhealthy shortly after promotion,
  this is usually a resource problem (OOMKilled, CPU throttling causing
  slow `PING` responses past the readiness probe's timeout) rather than a
  failover-logic problem — check `kubectl top pod` and `spec.resources`.
- If failover happens on brief, sub-second network blips, raise
  `spec.failover.unhealthyThresholdSeconds` (default 30s already requires
  sustained unreadiness, but very noisy CNI setups may need more).
- To stop automatic failover entirely while you debug: set
  `spec.failover.enabled: false`, fix the underlying issue, then
  re-enable.

## Backup CronJob failing

```bash
kubectl get jobs -l kividb.io/cluster=my-cluster
kubectl logs job/<latest-job-name>
kubectl get kdbs -l kividb.io/cluster=my-cluster --sort-by=.status.startTime
kubectl get kdbs <latest-snapshot-name> -o jsonpath='{.status.error}'
```

Common errors and what they mean:

| Error contains | Likely cause |
|---|---|
| `connection refused` / `no such host` | The target Service (master or replica, per the `KividbSnapshotConfig`'s `spec.source`) has no matching pod right now — check `status.masterPod` on the `KividbCluster`, or that `spec.replicas >= 1` if `source: replica` |
| `AccessDenied` / `SignatureDoesNotMatch` | Wrong S3 credentials, or wrong `region`/`endpoint` combination |
| `bucket does not exist` | `KividbSnapshotConfig`'s `spec.s3.bucket` typo, or bucket not created ahead of time (the operator never creates buckets) |
| `timed out waiting for BGSAVE` | Data set too large for `spec.timeoutSeconds`, or disk I/O contention — check `spec.resources` and node disk pressure |
| `no persistence files found` | Neither `dump.kdb` nor `appendonly.aof` exists yet on a brand-new, still-empty cluster — expected on the very first backup of an empty database; write some data first if this is unexpected |

If no `KividbSnapshot` object was created at all for a run you know
happened, check that the CronJob's pod actually completed (even a
crashed/OOMKilled pod should still leave a Job the controller can read a
termination message from) — a `KividbSnapshot` only fails to appear if
the controller itself couldn't reach the Job/pod objects, which usually
means an RBAC or API-server connectivity problem with the operator
itself, not the backup Job.

See [BACKUP_RESTORE.md](BACKUP_RESTORE.md) for how to trigger a manual
backup while debugging.

## ACL / authentication errors (`NOAUTH`, `WRONGPASS`)

The operator renders the referenced `KividbAclConfig` into the
`<cluster>-auth` Secret (`users.acl` key) every reconcile. If you rotate
a password:

1. Update the Secret your `passwordSecretRef` points at.
2. The operator will notice on its next reconcile (it re-resolves every
   referenced Secret's value every pass) and rewrite `<cluster>-auth`.
3. **The mounted Secret volume on already-running pods updates
   automatically** (kubelet syncs Secret volumes, typically within ~60s),
   but kividb only re-reads the ACL file when told to — the operator does
   *not* currently call `ACL LOAD` automatically after a password
   rotation. Trigger it yourself per pod:

   ```bash
   kubectl exec my-cluster-0 -c agent -- wget -qO- --post-data='' http://localhost:8081/acl/reload
   ```

   or simply roll the pods (`kubectl rollout restart statefulset
   my-cluster`) if a brief one-at-a-time restart is acceptable.

If the **agent itself** can't authenticate to kividb (visible as
`/readyz`/`/status` calls failing with an auth error in agent logs), check
that `KIVIDB_AUTH_PASSWORD` on the `agent` container matches the
**default** user's current password — see
[CONFIGURATION.md](CONFIGURATION.md#kividbaclconfig) for how the default
user's password is derived.

**If a user can run a command you configured a negative `commandRules`
entry to block** (e.g. `-flushall` didn't stop `FLUSHALL`): this is not a
Secret/rendering problem. Live testing has confirmed kividb currently
accepts and correctly echoes back negative command rules via
`ACL GETUSER` without enforcing them — see
[ROADMAP.md](ROADMAP.md#known-upstream-kividb-issues-confirmed-by-live-testing-2026-07-23).
Nothing on the operator side to fix here today.

## TLS: nothing is listening on `spec.tls.port`

## TLS: nothing is listening on `spec.tls.port`

**On kividb v1.0.3+** (`-tls`/`-full` images), the TLS listener should
come up when `KividbConfig.spec.tls.enabled: true` and `variant` is
`tls`/`full`. Confirm with:

```bash
kubectl exec <pod> -c kividb -- cat /proc/net/tcp
# look for a LISTEN entry on the tls-port (default 6443 → hex 0x192B)
```

**On kividb v1.0.2**, a missing TLS listener was expected — not an
operator configuration mistake. Live testing then confirmed the listener
never came up regardless of conf vs CLI flags (the operator sends both).

If TLS is missing on v1.0.3+, first check the Secret mount:

```bash
kubectl exec <pod> -c kividb -- ls -la /etc/kividb/tls
```

`tls.crt`/`tls.key` should resolve. Also check for a
`TLSVariantMismatch` Event (`kubectl describe kividbcluster`). See
[ROADMAP.md](ROADMAP.md#known-upstream-kividb-issues).

## Operator ImagePullBackOff on Apple Silicon / arm64

**Symptom:** after upgrading the Helm chart to `0.2.0`, manager/GUI pods
stuck in `ImagePullBackOff` with:

```text
no matching manifest for linux/arm64/v8 in the manifest list entries
```

**Cause:** the `0.2.0` images on Quay were published as an OCI index
containing only `linux/amd64` (plus an attestation manifest with
`unknown/unknown`). Container runtimes that select by host architecture
(minikube on Apple Silicon) refuse to pull. `0.1.0` was a single-arch
amd64 image and often still ran via emulation, which is why the first
install worked and the upgrade did not.

**Fix:** install operator **0.3.0+** (multi-arch `linux/amd64` +
`linux/arm64`), or for local development build and load arm64 images:

```bash
make docker-build VERSION=0.3.0-local
minikube image load quay.io/kividbio/kividb-operator:0.3.0-local
minikube image load quay.io/kividbio/kividb-operator-agent:0.3.0-local
minikube image load quay.io/kividbio/kividb-operator-gui:0.3.0-local
helm upgrade --install kividb-operator charts/kividb-operator \
  -n kividb-operator-system \
  --set manager.image.tag=0.3.0-local \
  --set gui.image.tag=0.3.0-local \
  --set manager.image.pullPolicy=IfNotPresent \
  --set gui.image.pullPolicy=IfNotPresent
```

## Storage: PVC won't resize after changing `spec.storage.size`

This is a known limitation, not a bug to report: `VolumeClaimTemplates`
are immutable on an existing `StatefulSet`, so the operator cannot
propagate a `spec.storage.size` change automatically. If your
StorageClass supports volume expansion (`allowVolumeExpansion: true`):

```bash
for pvc in $(kubectl get pvc -l kividb.io/cluster=my-cluster -o name); do
  kubectl patch "$pvc" -p '{"spec":{"resources":{"requests":{"storage":"20Gi"}}}}'
done
# Depending on your CSI driver, pods may need a restart to see the new size:
kubectl rollout restart statefulset my-cluster
```

If your StorageClass doesn't support expansion, you'll need to provision
larger PVCs and restore from backup (see BACKUP_RESTORE.md) instead.

## The operator pod itself won't start

```bash
kubectl logs -n kividb-operator-system deploy/kividb-operator-manager
```

- `leader election lost` / repeated restarts with leader-election errors
  — check that the `coordination.k8s.io` `leases` RBAC
  (`config/rbac/leader_election_role.yaml`) is bound in the same
  namespace the operator runs in.
- `no matches for kind "KividbCluster"` (or `KividbConfig`/
  `KividbAclConfig`/`KividbSnapshotConfig`/`KividbSnapshot`) — that CRD
  isn't installed; run `kubectl apply -k config/crd` (installs all five)
  or reinstall/upgrade the Helm chart (CRDs in
  `charts/kividb-operator/crds/` are installed once by Helm and are
  **not** upgraded automatically on `helm upgrade` — see
  [RELEASING.md](RELEASING.md) and [INSTALL.md](INSTALL.md) for the
  manual CRD-upgrade step).
- `KividbCluster ... configRef/aclConfigRef/snapshotConfigRef` reconcile
  error naming a resource that doesn't exist — the referenced
  `KividbConfig`/`KividbAclConfig`/`KividbSnapshotConfig` object was
  never created, or was created in a different namespace (references are
  same-namespace only). `kubectl get kdbc,kdbacl,kdbsc -n <namespace>` to
  check what actually exists.

## Still stuck?

Open an issue at
https://github.com/kividbio/kividb-operator/issues with: `kubectl get
kividbcluster <name> -o yaml`, `kubectl describe pod` for the affected
pod(s), and the operator's logs around the time of the problem.
