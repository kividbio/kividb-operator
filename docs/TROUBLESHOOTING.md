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
  wget -qO- http://<pod-ip>:8081/readyz
```

(`/readyz` needs no token. `/status` and the other endpoints answer 401
without the agent token: that is expected, not a network problem.)

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

## Failover does not happen (`FailoverBlocked`)

The master is down, the replicas are Ready, and the cluster stays in
`Error` with a `FailoverBlocked` event. The operator found that no ready
replica holds any keys although the master was last seen holding data
(compare `status.pods[].keys`). On kividb before v1.0.5 a replica could be
emptied by a master that keeps restarting, all of them at once (see
[KIVIDB_ENGINE_ISSUES.md](KIVIDB_ENGINE_ISSUES.md), issue 10).

The data is then only on the master's volume, so the operator waits for
that pod instead of promoting an empty replica, which would make the
master resync from it and lose the data for good when it returned. Fix
whatever is keeping the master down (`kubectl describe pod`, most often
an OOM kill: raise `spec.resources.limits.memory`); once it is Ready the
replicas resync from it.

If the master's data is truly gone and an empty cluster is acceptable:

```bash
kubectl annotate kividbcluster my-cluster kividb.io/allow-empty-failover=true
```

Remove the annotation again afterwards.

## Failover held for a replica that did not answer (`FailoverBlocked`, `WaitingForReplica`)

The master is gone, and a Ready replica did not say how much it holds
(`ROLE`, asked through its agent) within 1 s. It may hold writes the
others do not, so no one is promoted; the event says how long the failover
has been held. A replica stays Ready while the operator cannot reach it,
because its readiness probe runs from the kubelet on its own node: check
NetworkPolicies and whether the operator's pod reaches the replica's agent
on port 8081. Then fix the network, or delete the replica's pod, or, if
losing any writes only that replica holds is acceptable:

```bash
kubectl annotate kividbcluster my-cluster kividb.io/allow-unread-failover=true
```

The operator removes the annotation once the failover has gone through.

## Failover waiting for a master no replica was in sync with (`FailoverBlocked`, `WaitingForMaster`)

The master does not answer, its pod gone or restarting in place, and no
Ready replica was in sync with it when it went: one in the middle of a
full resync, say, or cut off from it. The master's volume may hold
acknowledged writes that no replica has, so the failover waits for it to
come back, however long that takes, and then keeps it. The Ready
condition's message says the same,
after the event has expired. Check why it is not back:
`kubectl describe pod` shows a node that is not Ready, a volume that
cannot attach (a Multi-Attach error), or the kividb container's restarts
and last state (an OOM kill: raise `spec.resources.limits.memory`). If the
master's node or volume is gone for good, or losing the writes it
acknowledged since the replica was last in sync is acceptable:

```bash
kubectl annotate kividbcluster my-cluster kividb.io/allow-unsynced-failover=true
```

The failover then goes ahead (a `FailoverForced` Warning; for a master
whose pod is gone, once `failover.restartWaitSeconds` have passed), and the
operator removes the annotation.

With `WaitingForReplica` instead, the master still answers, but no
replica is near it (within 1 MiB, its link up), or the one picked did not
catch up within 5 s, whether or not it was in sync when the master became
unavailable. The master keeps serving; its writes are held for
a catch-up only once a replica is within 1 MiB of it. Watch the replica's
sync (`INFO replication` on it: `master_link_status`,
`master_sync_in_progress`), or find why its link is down. The same
annotation lets the failover go ahead.

## Failover without a fence (`FailoverUnfenced`)

A failover went ahead without first holding the old master's writes, or
promoted a replica before it had caught up with the old master. A master
that was still running then -- a force-deleted pod's container, for a
moment -- may have acknowledged writes the new master never got. The
event says why:

- **No address or engine run recorded** (`status.pods[].podIP`, `runID`).
  The usual cause is a `kividbclusters` CRD older than the operator: Helm
  installs CRDs once and never upgrades them, and the API server drops
  the status fields its CRD does not list. Apply the current CRDs:

  ```bash
  kubectl apply -f charts/kividb-operator/crds/
  ```

  The address is recorded again from the next status write. Right after
  an upgrade, pods whose agent has not been replaced yet report no engine
  run either; the rollout replaces them.
- **No answer** at the old master's address: nothing listens there, or
  nothing answered for about 1 s -- its node is down, or cut off from the
  operator.
- **No pause within 3 s**, or **the engine refused `CLIENT PAUSE WRITE`**
  (kividb before 1.1).
- **Promoting a replica before it caught up**, with
  `kividb.io/allow-unsynced-failover` set: it was still behind after 5 s,
  or the old master stopped answering first.

A `FencedMasterNotDemoted` Warning means the old master was held but could
not yet be made the new master's replica. Its writes stay held, and the
operator tries again every 2 s (`status.fencedMaster` shows it) until it
is done or the old engine is gone.

## `PersistenceDisabled`, `MasterRestartedEmpty`, `ShardDataLost`

- **`PersistenceDisabled`** (and the `Persistence` condition `False`): the
  cluster's `KividbConfig` sets `appendonly no`. The engine never
  snapshots on a schedule, so a pod whose engine is killed restarts with
  what it saved at its last clean shutdown or full sync, often nothing.
  Remove the directive (the operator then writes `appendonly yes`) unless
  the data can be lost. See
  [CONFIGURATION.md](CONFIGURATION.md#persistence-the-append-only-file).
- **`MasterRestartedEmpty`** (sharded): a shard master restarted with less
  than half the keys its replica holds, typically without the append-only
  file. The operator holds the master's writes and has the replica take
  over (`CLUSTER FAILOVER FORCE`, then `TAKEOVER`);
  `MasterRestartedEmptyResolved` follows when it has. If the condition
  `MasterRestartedEmpty` says `Stalled`, the replica did not take over
  after 6 requests: check its log, and if it still holds the data, run
  `CLUSTER FAILOVER TAKEOVER` on it yourself.
- **`ShardDataLost`** (sharded): a shard master restarted with less than
  half of what it last reported and no replica of the shard holds more,
  or the replica resynced from it before it could take over. The writes
  it lost are only in a backup now. Turn the append-only file back on.

## A rolling restart stops moving (`RolloutWaiting`)

The cluster is `Running` and `Ready`, but pods stay on the old template.
A rollout only replaces the next pod while every other pod is Ready and
every replica in sync, so a replica that never catches up holds it,
cluster-wide in a sharded cluster. The `RolloutProgressing` condition
names the pod and why (`kubectl describe kividbcluster my-cluster`), and
after 2 minutes a `RolloutWaiting` Warning Event says the same:

- **link down**: the replica cannot reach or authenticate to its master;
  look at its engine log (`kubectl logs <pod> -c kividb`).
- **link up, bytes behind, last I/O long ago**: the replica is connected
  but applies nothing; restarting that pod (`kubectl delete pod <pod>`)
  lets it resync, and the rollout goes on. Report it as an engine issue.
- **first full sync not finished**: a large dataset is still loading;
  wait.

## A rollout holds a shard master (`RolloutHeld`, `WouldLoseData`)

A sharded cluster with `replicas: 0` and the append-only file off: a
shard master has no replica to hand over to, so the rollout would restart
it in place, and it would keep only what its save at shutdown writes in
time. The rollout holds it instead and goes on with the other masters;
once only held ones are left, the `RolloutProgressing` condition says
why. To go on:

- give the cluster a replica (`spec.replicas: 1`): once it is in sync,
  the master hands over and nothing is lost;
- or turn the file on: `appendonly yes` in the `KividbConfig`, and
  `CONFIG SET appendonly yes` on the running master, so that it is on
  before the restart;
- or, if the data can be lost, let the rollout restart the masters:

  ```bash
  kubectl annotate kividbcluster my-cluster kividb.io/allow-restart-without-persistence=true
  ```

  Set it after the hold appears: the operator removes it whenever no pod
  is on an old template, so set before the change it does nothing. Each
  restart raises a `RestartingWithoutPersistence` Warning.

## A KividbConfig `dir` off the volume (`InvalidSpec`)

A `dir` in the `KividbConfig` is where the engine keeps its snapshot and
append-only file: it changes into it at start. One that resolves outside
`/data`, the pod's volume (`/data/../tmp` included), is refused: `Ready`
is `False`, reason `InvalidSpec`, naming the `KividbConfig` and the path.
While it shows, nothing of the cluster is applied, not this change nor any
other (an image, a replica count, a backup schedule), so no pod is
restarted onto it; failover goes on. Set a `dir` under `/data` (a
relative one resolves against it), or remove it.

A cluster that already ran with such a `dir` keeps its data in memory and
on each container's own filesystem, not on the volume: a pod that
restarts for any reason (a drain, an eviction, an out-of-memory kill)
comes back without it, and backups find nothing to copy. The operator
restarts nothing for it. To get out:

- **With replicas:** wait until every pod is Ready and every replica in
  sync (in every shard, in a sharded cluster), then fix `dir`. The rollout
  restarts the replicas first; each comes back on the volume and copies
  the data from its master, and each master hands over to one of them
  before its own restart. A pod that has been unready for 30 s on the old
  template is replaced without that wait, so do not fix `dir` while one
  is.
- **With no replicas:** take a copy of each master first, then fix `dir`;
  if the restart loses the data, restore the copy. The engine's image has
  no shell, so take it from another pod with `redis-cli --rdb`, adding
  `--user default --pass <password>` if the default user has one, and
  `--tls --cacert <CA file>` with the TLS port if the plain port is off
  (the CA file has to be in the copy pod):

  ```bash
  kubectl -n my-namespace run kividb-copy --image=redis:8 --restart=Never -- sleep infinity
  kubectl -n my-namespace exec kividb-copy -- \
    redis-cli -h my-cluster-0.my-cluster-headless -p 6380 --rdb /tmp/dump.kdb
  kubectl -n my-namespace cp kividb-copy:/tmp/dump.kdb dump.kdb
  kubectl -n my-namespace delete pod kividb-copy
  ```

  While it runs, the master counts the copy as a replica, for as long as
  its full sync takes. In a sharded cluster, take one copy per shard, from
  the shard's current master: its pods are `<cluster>-<shard>-<n>` under
  `<cluster>-<shard>-headless`, and the master is the one labelled
  `kividb.io/role=master` (`kubectl get pods -l
  kividb.io/cluster=my-cluster,kividb.io/role=master`), not always `-0`.
  The copy is a `dump.kdb`, the file a backup holds: restore it as one
  (see [BACKUP_RESTORE.md](BACKUP_RESTORE.md#restoring-a-backup)).

## ACL / authentication errors (`NOAUTH`, `WRONGPASS`)

The operator renders the referenced `KividbAclConfig` into the
`<cluster>-auth` Secret (`users.acl` key) every reconcile. If you rotate
a password:

1. Update the Secret your `passwordSecretRef` points at.
2. The operator will notice on its next reconcile (it re-resolves every
   referenced Secret's value every pass) and rewrite `<cluster>-auth`.
3. The mounted Secret volume on already-running pods updates on its own
   (kubelet syncs Secret volumes, typically within a minute or two). Once
   a pod has the new file, the operator has its agent run `ACL LOAD` and
   records that on the pod as the `kividb.io/acl-generation` annotation.
   Until then the **old** passwords are still the ones kividb accepts.
4. If the password that changed is the **`default` user's**, the operator
   rolls the pods instead (one at a time, like an image change): the agent
   and exporter sidecars authenticate with that password and only read it
   at startup.

To see whether a pod has caught up, compare its annotation with the one
on the `<cluster>-auth` Secret:

```bash
kubectl get secret my-cluster-auth -o jsonpath='{.metadata.annotations.kividb\.io/acl-generation}'
kubectl get pods -l kividb.io/cluster=my-cluster \
  -o custom-columns=POD:.metadata.name,ACL:.metadata.annotations.kividb\\.io/acl-generation
```

A pod that stays behind has an `AclReloadFailed` event on the
`KividbCluster` saying why.

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
minikube image load docker.io/kividb/kividb-operator:0.3.0-local
minikube image load docker.io/kividb/kividb-operator-agent:0.3.0-local
minikube image load docker.io/kividb/kividb-operator-gui:0.3.0-local
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
