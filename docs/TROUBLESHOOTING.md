# Troubleshooting

Start with the cluster's own status — it's usually the fastest path to a
diagnosis:

```bash
kubectl get kividbcluster my-cluster -o yaml
kubectl describe kividbcluster my-cluster   # shows Conditions + recent Events
kubectl get pods -l kividb.io/cluster=my-cluster -o wide --show-labels
kubectl logs deploy/kividb-operator-controller-manager -n kividb-operator-system
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

  If that fails with an auth error, your `spec.auth` password Secret and
  the agent's `KIVIDB_AUTH_PASSWORD` env var have drifted — see the ACL
  section below.

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
kubectl exec -n kividb-operator-system deploy/kividb-operator-controller-manager -- \
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
kubectl get kividbcluster my-cluster -o jsonpath='{.status.backup.lastError}'
```

Common errors and what they mean:

| Error contains | Likely cause |
|---|---|
| `connection refused` / `no such host` | Master Service DNS wrong, or no pod currently holds the master label (check `status.masterPod`) |
| `AccessDenied` / `SignatureDoesNotMatch` | Wrong S3 credentials, or wrong `region`/`endpoint` combination |
| `bucket does not exist` | `spec.backup.s3.bucket` typo, or bucket not created ahead of time (the operator never creates buckets) |
| `timed out waiting for BGSAVE` | Data set too large for the default 5-minute save window, or disk I/O contention — check `spec.resources` and node disk pressure |
| `no persistence files found` | Neither `dump.kdb` nor `appendonly.aof` exists yet on a brand-new, still-empty cluster — expected on the very first backup of an empty database; write some data first if this is unexpected |

See [BACKUP_RESTORE.md](BACKUP_RESTORE.md) for how to trigger a manual
backup while debugging.

## ACL / authentication errors (`NOAUTH`, `WRONGPASS`)

The operator renders `spec.auth` into the `<cluster>-auth` Secret
(`users.acl` key) every reconcile. If you rotate a password:

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
[CONFIGURATION.md](CONFIGURATION.md#auth--requirepass-and-acl-users) for
how the default user's password is derived.

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
kubectl logs -n kividb-operator-system deploy/kividb-operator-controller-manager
```

- `leader election lost` / repeated restarts with leader-election errors
  — check that the `coordination.k8s.io` `leases` RBAC
  (`config/rbac/leader_election_role.yaml`) is bound in the same
  namespace the operator runs in.
- `no matches for kind "KividbCluster"` — the CRD isn't installed; run
  `kubectl apply -k config/crd` or reinstall/upgrade the Helm chart (CRDs
  in `charts/kividb-operator/crds/` are installed once by Helm and are
  **not** upgraded automatically on `helm upgrade` — see
  [RELEASING.md](RELEASING.md) and [INSTALL.md](INSTALL.md) for the
  manual CRD-upgrade step).

## Still stuck?

Open an issue at
https://github.com/kividbio/kividb-operator/issues with: `kubectl get
kividbcluster <name> -o yaml`, `kubectl describe pod` for the affected
pod(s), and the operator's logs around the time of the problem.
