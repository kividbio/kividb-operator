# Field reference

Group/version: `kividb.io/v1alpha1`. Five kinds, all namespaced:

| Kind | Short name | Purpose |
|---|---|---|
| [`KividbCluster`](#kividbcluster) | `kdb` | The cluster itself: topology, storage, scheduling, Services, monitoring, failover. References the three kinds below by name. |
| [`KividbConfig`](#kividbconfig) | `kdbc` | Reusable `kividb.conf` directives + TLS settings. |
| [`KividbAclConfig`](#kividbaclconfig) | `kdbacl` | Reusable ACL users / `requirepass`. |
| [`KividbSnapshotConfig`](#kividbsnapshotconfig) | `kdbsc` | Reusable backup schedule, S3 destination, and retention policy. |
| [`KividbSnapshot`](#kividbsnapshot-read-only) | `kdbs` | Operator-created record of one backup run. Read-only. |

This split mirrors StackGres's `SGCluster` / `SGPostgresConfig` /
`SGObjectStorage` model: configuration, ACLs, and backup destinations are
independent objects you create once and reference by name from any number
of clusters, instead of fields embedded on the cluster itself. None of
this configuration lives in a ConfigMap or Secret you manage by hand — the
operator renders those from these CRDs and owns them entirely.

See [config/samples/](../config/samples/) for complete, applyable
examples of every kind below, including a full multi-CRD cluster and a
TLS-enabled one.

## KividbCluster

All fields below are under `spec:` unless stated otherwise.

### Core

| Field | Type | Default | Notes |
|---|---|---|---|
| `shards` | int32 | unset | Makes the cluster **sharded**: that many shards in kividb's cluster mode. See [Sharded clusters](#sharded-clusters-shards). Set or left unset only when the cluster is created. |
| `replicas` | int32 | `2` | Replica pods **in addition to** each master: beside the single master (total pods = `replicas + 1`), or in each shard (total = `shards × (replicas + 1)`). |
| `image` | string | `docker.io/kividb/kividb:v1.1.2` | Explicit kividb container image, e.g. `docker.io/kividb/kividb:v1.1.2` or `docker.io/kividb/kividb:v1.1.2-tls` (the same tags are on `ghcr.io/kividbio/kividb`). Used verbatim — see [Image and variant](#image-and-variant) for why the operator never constructs or modifies this value itself. |
| `variant` | string | `standard` | One of `standard`, `tls`, `lua`, `full`. See [Image and variant](#image-and-variant) — informational only, does not affect which image gets pulled. |
| `imagePullPolicy` | string | `IfNotPresent` | `Always`, `IfNotPresent`, or `Never`. |
| `imagePullSecrets` | `[]LocalObjectReference` | — | Standard Kubernetes image pull secrets. |
| `agentImage` | string | the operator's bundled agent image | Override only if you build your own agent image. |
| `exporterImage` | string | `oliver006/redis_exporter:v1.66.0` | Only used when `monitoring.enabled: true`. See [`monitoring`](#monitoring). |
| `port` | int32 | `6380` | RESP port. Also written into the rendered `kividb.conf` as `port`. |
| `terminationGracePeriodSeconds` | int64 | `60` | kividb snapshots on SIGTERM; keep this comfortably above your data size's `SAVE` time. A stopping master first uses up to a third of it (5 to 30 s) to hand its role to a replica, see [Switchovers](#switchovers-and-stopping-a-master). |

### Image and variant

`image` is the **only** field that determines which container actually
runs — the operator uses it verbatim and never constructs, guesses, or
modifies it. Leave it unset to use the release default
(`docker.io/kividb/kividb:v1.1.2` for operator 0.6.0); set it explicitly
to pin a different tag or variant:

```yaml
spec:
  image: docker.io/kividb/kividb:v1.1.2-tls
```

To move to a later version, just change this value — same as any other
Kubernetes Deployment/StatefulSet image bump (a rolling pod-by-pod
restart, since `VolumeClaimTemplates`/data are untouched).

Editing the referenced `KividbConfig` does the same: kividb only reads
`kividb.conf` at startup, so a change to its directives rolls the pods
one at a time (the pod template carries a `kividb.io/config-hash`
annotation for exactly this purpose).

The operator performs these rollouts itself (the StatefulSet uses the
`OnDelete` update strategy): replicas first, the master last, and the
next pod is only replaced once every pod is Ready and every replica shows
`synced: true` in `status.pods`. When only the master is left, its role
is first handed to an in-sync replica with a lossless switchover (a
`Switchover` event, see [Switchovers](#switchovers-and-stopping-a-master))
and the old master is replaced afterwards, as a replica. A pod that is unready and still on the
old template is replaced without waiting, so a change that fixes a
crash-looping pod is not held up by that pod.

While pods are on an outdated template, the condition
`RolloutProgressing` says where the rollout is. `True`, reason
`ReplacingPod`: it has just replaced a pod (or moved the master role to
replace it next). `True`, reason `WaitingForReplicaSync` (a Ready replica
is not in sync) or `WaitingForPodReady`: it waits, as it does after every
replacement while the new pod syncs, and the message names each pod and
why (its link up or down, how many bytes it is behind the master, when it
last heard from it). After 2 minutes of waiting on the same pods the
condition turns `False` with the same reason, and a `RolloutWaiting`
Warning Event and a log line say the same, again every 10 minutes while
it lasts. Nothing else changes: the rollout still waits, and `Ready`
still describes the cluster's service. The condition goes once every pod
is on the current template. Changes to a `KividbAclConfig` are
applied without a restart unless the `default` user's password changed —
see [Changing the default user's password](#changing-the-default-users-password)
and [TROUBLESHOOTING.md](TROUBLESHOOTING.md#acl--authentication-errors-noauth-wrongpass).

`variant` is a **separate, informational** field — it tells the operator
which kind of build `image` is (`standard`, `tls`, `lua`, or `full`
[TLS+Lua]), so the operator knows whether to wire up variant-specific
pod configuration (currently: TLS cert mounting and CLI flags, driven by
a referenced [`KividbConfig`](#kividbconfig)'s `spec.tls`). It does
**not** change which image tag gets pulled — there is no "tls variant
tag" the operator appends or looks for. If you want a TLS-capable build,
you set `image` to one yourself (e.g.
`docker.io/kividb/kividb:v1.1.2-tls`) *and* set `variant: tls` to match;
the two aren't cross-validated by the API server, since only you know
what a given `image` value actually contains.

The operator surfaces a best-effort check as a **Kubernetes Event**
(visible via `kubectl describe kividbcluster <name>`) rather than a hard
validation error, so a mismatch never blocks reconciling:

- `variant` set to anything other than `standard` → a `Normal`
  `VariantGuidance` event reminding you to confirm `image` actually
  matches.
- A referenced `KividbConfig` has `spec.tls.enabled: true` but `variant`
  is left at `standard` (the default) → a `Warning`
  `TLSVariantMismatch` event — this combination almost certainly means
  the TLS listener won't come up.

Setting `variant: tls`/`full` and pointing `image` at a build that
actually supports it still isn't sufficient today — see the live-tested
caveat below.

> **TLS by engine version:** e2e against `v1.0.3-tls` / `v1.0.3-full`
> confirmed the TLS port is in `LISTEN` when the operator mounts certs
> and passes CLI flags (see `hack/e2e/04-compat-variants.sh`). On
> **v1.0.2** the listener never opened — pin `spec.image` to v1.0.3+ if
> you need TLS. Engines from 1.1.0 read the `tls-*` directives from the
> config file; the operator writes them there and also passes them as
> CLI flags, for the older engines whose `--configfile` dropped them.
> Engine 1.2 reloads certificates with `CONFIG SET` (and on `CONFIG SET
> tls-cert-file` of the same path, a certificate renewed in place), with
> no restart (kividbio/kividb#156); the operator does not use that yet,
> so a renewed certificate takes effect when a pod restarts. Track
> remaining upstream caveats in
> [ROADMAP.md](ROADMAP.md#known-upstream-kividb-issues).

### References to other CRDs

```yaml
spec:
  configRef:
    name: my-cluster-config          # KividbConfig
  aclConfigRef:
    name: my-cluster-acl             # KividbAclConfig
  snapshotConfigRef:
    name: my-cluster-backups         # KividbSnapshotConfig
```

All three are optional `LocalObjectReference`s (name-only, same
namespace):

- Omit `configRef` for kividb's own built-in defaults plus whatever the
  operator itself pins (`port`, `aclfile`, and TLS directives if
  applicable).
- Omit `aclConfigRef` for an open, passwordless default user — fine for
  local/dev, not for anything reachable outside the cluster.
- Omit `snapshotConfigRef` to disable scheduled backups entirely.

If a reference names an object that doesn't exist, reconciliation fails
with a clear error surfaced on `status.conditions` — the cluster does not
silently fall back to defaults for a *typo'd* reference (only for an
*absent* one).

### `storage`

```yaml
spec:
  storage:
    size: 10Gi
    storageClassName: standard-rwo   # omit to use the cluster default
    accessModes: ["ReadWriteOnce"]   # default if omitted
```

One PVC per pod, mounted at `/data` (kividb's working directory — see
[ARCHITECTURE.md](ARCHITECTURE.md) for why). **Changing `size` after
creation is not currently propagated** — `VolumeClaimTemplates` are
immutable on an existing StatefulSet; see TROUBLESHOOTING.md for the
manual resize procedure.

### Scheduling: `tolerations`, `nodeSelector`, `affinity`

```yaml
spec:
  tolerations:
    - key: "workload"
      operator: "Equal"
      value: "database"
      effect: "NoSchedule"
  nodeSelector:
    disktype: ssd
```

Standard Kubernetes `Toleration[]`/`map[string]string`/`Affinity`,
applied verbatim to every pod. If `affinity` is omitted, the operator
injects a *preferred* (not required) pod anti-affinity rule spreading the
cluster's pods across distinct nodes by `kubernetes.io/hostname` — set
your own `affinity` to override this entirely (there is no way to keep the
default and add to it; an explicit `affinity` replaces it).

### `resources` / `agentResources` / `exporterResources`

Standard Kubernetes `ResourceRequirements` for the `kividb` container, the
`agent` sidecar, and the `redis-exporter` sidecar (only relevant when
`monitoring.enabled: true`) respectively:

```yaml
spec:
  resources:
    requests: {cpu: "500m", memory: "1Gi"}
    limits: {cpu: "2", memory: "2Gi"}
  agentResources:
    requests: {cpu: "10m", memory: "32Mi"}
    limits: {cpu: "100m", memory: "128Mi"}
  exporterResources:
    requests: {cpu: "10m", memory: "32Mi"}
    limits: {cpu: "100m", memory: "128Mi"}
```

**Set a memory request and limit for `kividb`, and `maxmemory` below the
limit** (in a `KividbConfig`). Without them the scheduler cannot tell how
much a pod needs, and pods packed onto one node can drive it out of
memory together. A node's out-of-memory killer then picks a kividb, most
likely the one using the most. Leave headroom above `maxmemory` for a full
resync, a backup, and in a sharded cluster a slot move, which each hold
extra copies for a while.

### `services` — master and replica exposure

```yaml
spec:
  services:
    master:
      type: LoadBalancer
      loadBalancerIP: "203.0.113.10"        # cloud-provider support varies
      loadBalancerSourceRanges: ["10.0.0.0/8"]
      annotations:
        service.beta.kubernetes.io/aws-load-balancer-type: "nlb"
    replicas:
      type: ClusterIP   # read-scaling within the cluster; not exposed externally
```

Both default to `ClusterIP` if `type` is omitted. The master Service
(`<cluster>-master`) always selects whichever pod currently holds
`kividb.io/role: master` — no changes needed on failover. The replica
Service (`<cluster>-replicas`) load-balances reads across all replicas.
There's also always a headless Service (`<cluster>-headless`, not
configurable) backing the StatefulSet's stable pod DNS names.

The master and replica Services carry the kividb port and, when the
cluster's `KividbConfig` enables TLS, the TLS port (named `tls`). Only the
headless Service also carries the agent port (8081): the agent's API,
which needs the agent token, is never on a Service that clients or the
outside reach.

### `monitoring`

```yaml
spec:
  monitoring:
    enabled: true          # adds a redis_exporter sidecar (port 9121)
    serviceMonitor: true   # also create a Prometheus Operator ServiceMonitor
```

`enabled: true` adds a third container, `redis-exporter`
(`oliver006/redis_exporter`, see [`exporterImage`](#core) /
[`exporterResources`](#resources--agentresources--exporterresources)), to
every pod in the cluster, listening on port 9121. It's the de facto
standard Prometheus exporter for Redis-protocol stores, and pairs with
the commonly used Redis Grafana dashboards without extra relabeling. The
agent sidecar's own lightweight `/metrics` on port 8081 (derived from
`INFO`) is always available regardless of this setting — `enabled` only
gates the heavier, richer exporter.

`serviceMonitor: true` requires the `monitoring.coreos.com` CRDs
(Prometheus Operator) to already be installed in the cluster. The
generated `ServiceMonitor` (one per operator install, cluster-wide, not
per-`KividbCluster`) scrapes both the agent's `/metrics` and, where
present, `redis-exporter`'s `/metrics`.

### `failover`

```yaml
spec:
  failover:
    enabled: true                    # default
    unhealthyThresholdSeconds: 30    # default
```

Set `enabled: false` to disable automatic promotion entirely (you'll need
to call the agent's `/promote` and `/replicaof` endpoints yourself, or
patch the `kividb.io/role` pod label and expect the controller to leave it
alone). Lowering `unhealthyThresholdSeconds` makes failover more
aggressive at the cost of being more sensitive to transient blips (a slow
`BGSAVE`, a brief network hiccup). Keep it below about 75 s: the kividb
container's liveness probe restarts an engine that has answered nothing
for about 90 s, and a hung master should be failed over before that, not
come back as the master (see [Liveness](#liveness)).

**What a failover can lose.** Replication is asynchronous: a master
acknowledges a write before its replicas have it. When a master crashes
or its pod is force-deleted, the writes it acknowledged in its last moments
and had not yet streamed are lost with it, and a master that is cut off
(not dead) goes on acknowledging writes on the connections it still has
until it is demoted -- in the AKS test, 150 ms after the promotion, 120
writes. The operator does not fence it. `min-replicas-to-write` /
`min-replicas-max-lag` narrow this window: a master refuses writes
(`NOREPLICAS`) unless that many replicas acknowledged within the lag. The
operator does not set them in a replicated cluster (it does in a sharded
one, through `spec.cluster.minReplicasToWrite`, default 0), because the
trade-off is availability: with `min-replicas-to-write 1`, a cluster with
one replica stops taking writes whenever that replica restarts -- every
rolling update -- and a master cut off from its replicas stops at once
instead of after the lag. Set them in a [`KividbConfig`](#kividbconfig)
if refusing writes is preferable to losing them; the lag bounds the loss
to about that many seconds, it does not remove it.

**When the master restarts behind its replicas.** A master that restarts
-- crashed, OOM-killed, deleted while it could not hand over -- comes back
from its last snapshot, which may be older than its last acknowledged
writes, while its replicas still hold them. If the replicas resynced from
it, those writes would be gone from every copy. The operator recognises
it -- the master's engine restarted (a new `run_id`, or a new pod or
container restart), it came back below the offset it had reached in its
stream or with a new stream, and a replica that was in sync is ahead --
and fails over to the replica holding the most instead; the restarted pod
becomes its replica (`MasterRestartedBehind` and `Failover` Events), with
its writes held until it is. A master that never restarted is not failed
away from on its offsets, however they compare. It works when the master
pod is recreated, as its replicas then follow its old address until the
operator points them anywhere. When only the container restarts, in
place, with the same address, the replicas reconnect to it on their own
within a second and the engine resyncs them before the operator can act:
that case needs the engine to refuse such a resync (a replica asking to
continue a stream from further than the master has it).

### Switchovers and stopping a master

A **switchover** moves the master role from a live master to a replica.
The operator does one when a rollout or a `KividbDbOps` restart reaches
the master, when the master's pod is being deleted (by hand, a node drain,
an eviction), when a pod has the `kividb.io/step-down: "true"`
annotation, and when the cluster has the annotation
`kividb.io/switchover-to: <pod>` (the GUI's promote action sets it; the
operator removes it once that pod is the master, or drops it with a
`SwitchoverRequestDropped` Warning if there is no such pod):

```sh
kubectl annotate kividbcluster my-cluster kividb.io/switchover-to=my-cluster-2
```

It loses no acknowledged write:

1. The master stops acknowledging writes: `CLIENT PAUSE <ms> WRITE`.
   Clients' writes wait (reads go on); the pause has a 10 s timeout of its
   own.
2. The operator waits until the replica has applied everything the master
   has (`ROLE` on both: equal offsets, the master's no longer moving, the
   replica's link up), for at most **5 s**.
3. The replica is promoted, the old master becomes its replica, the master
   label (and with it the `<cluster>-master` Service) moves to the new
   master -- onto it before it comes off the old one, so the Service always
   has an endpoint -- and the other replicas follow the new master,
   continuing from where they were.
4. The old master's clients are disconnected so they reconnect through
   the Service, and the pause ends. A write that was waiting on the old
   master gets an error (`READONLY`, or a closed connection), never an
   acknowledgement, and a client that retries reaches the new master.

Writes wait for as long as the replica takes to catch up: milliseconds
for a replica in sync. If it has not caught up in 5 s, nothing changes:
the pause ends, the old master stays the master, a `SwitchoverAborted`
Warning says why, and the next attempt waits 30 s. A replica is a
candidate only when Ready and `synced`: following the master with
`master_link_status:up`, having heard from it in the last 20 s, and at
most 1 MiB behind. The most caught-up candidate is chosen, or the one
`kividb.io/switchover-to` names. On an engine before kividb 1.1, which has
no `CLIENT PAUSE ... WRITE`, the operator switches over without the pause
and says so (`SwitchoverWithoutPause` Warning): writes acknowledged by the
old master during the handover can be lost there.

**Stopping a master.** Both containers of every pod have a `preStop`
hook, the agent's `GET /prestop`. On a master with replicas attached it
waits until the role has moved -- the operator sees the pod being deleted
and switches over at once -- for at most a third of
`terminationGracePeriodSeconds`, between 5 and 30 s (20 s by default); then
the engine stops, as a replica, and saves. On anything else it returns at
once. A `PodDisruptionBudget` named after the cluster (`maxUnavailable:
1`) keeps a drain from evicting two of its pods at once, as each shard's
does in a sharded cluster. While the old master's pod is terminating it
is no longer an endpoint of the master Service; new connections fail for
the moment it takes the operator to move the label (well under a second
once it has seen the deletion). `kubectl delete --force --grace-period=0`
skips all of this: that is a crash, see above.

### Liveness

The kividb container's liveness probe is the agent's `GET /livez`, which
sends `PING` to the engine and passes on **any** reply within 3 s --
`LOADING`, `BUSY` and `NOAUTH` included. Before 0.7.0 it was a TCP check,
which a deadlocked engine passes for ever: the kernel accepts the
connection, and nothing answers. The probe runs every 10 s with a 5 s
timeout and restarts the container after 9 failures in a row, about 90 s
of no answer at all:

- long enough for a slow engine -- under load, loading a full sync,
  running a long script or a save -- since a reply of any kind passes;
- longer than readiness (15 s) plus the failover threshold (30 s), so a
  hung master is failed over first and comes back as a replica;
- and the agent, which proxies the probe, restarts within that time if it
  is the one that failed.

Until the engine has answered once, a startup probe on the same endpoint
stands in (every 10 s, up to 30 minutes), for an engine that loads a large
dataset before it listens. Both need the 0.7.0 agent image: an older
agent has no `/livez`, and the probe would fail.

### Sharded clusters: `shards`

With `spec.shards`, kividb runs in its cluster mode: the 16,384 hash slots
are spread over the shards, each a master and `replicas` replicas, and
clients use any Redis Cluster client.

```yaml
spec:
  shards: 3
  replicas: 1          # per shard: 6 pods
  storage: {size: 20Gi}
  cluster:
    nodeTimeoutMs: 5000          # cluster-node-timeout
    requireFullCoverage: true    # cluster-require-full-coverage
    minReplicasToWrite: 0        # min-replicas-to-write
    minReplicasMaxLagSeconds: 10 # min-replicas-max-lag
    # replicaValidityFactor: 10  # cluster-replica-validity-factor; engine 1.2 or later
    # busSecretRef: {name: my-bus, key: secret}   # generated into <cluster>-bus when unset
```

- **Fixed at creation.** A cluster is sharded, or not, for good:
  `spec.shards` cannot be added to or removed from an existing cluster.
  Its value can change: see "Changing the shard count" below.
- **At most 24 nodes** (`shards × (replicas + 1)`) for now, and a name of
  at most 40 characters. The cap dates from engines whose cluster bus
  opened a new connection for every heartbeat, N × (N − 1) a second
  across the cluster (kividbio/kividb#112). Engine 1.2 keeps one
  connection per peer, which removes that cost; the cap stays until
  larger clusters have been tested.
- **Workloads.** One StatefulSet per shard, `<cluster>-<shard>`, whose pods
  are `<cluster>-<shard>-<n>`, and a PodDisruptionBudget per shard
  (`maxUnavailable: 1`). Pods prefer other nodes than their shard's other
  pods, then than the cluster's.
- **Configuration.** The operator sets every cluster-mode directive
  (`cluster-*`, `min-replicas-*`, `masteruser`/`masterauth`, `replicaof`,
  `tls-cluster`); a `KividbConfig` that sets one is refused. The bus
  secret is always on. The exceptions are the slot migration settings,
  `cluster-slot-migration-handoff-max-lag-bytes` and
  `cluster-slot-migration-write-pause-timeout`, which a `KividbConfig` may
  set: the operator leaves them alone, and the engine applies them from
  the next migration's handoff (kividbio/kividb#111).
- **`replicaValidityFactor`** is `cluster-replica-validity-factor`
  (engine 1.2 or later). When a master fails, a replica whose link to it
  has been down for longer than `nodeTimeoutMs × replicaValidityFactor`
  (plus the replication ping period) does not stand for election: its
  data may be too old to take over with. Unset, the operator writes
  nothing and the engine's default, 10, applies, as in Redis: with the
  default node timeout of 5 s, a replica cut off from its master for more
  than about a minute does not take over, and a shard whose replicas are
  all that stale stays down until its master is back. `0` lets every
  replica stand, however far behind; a larger factor tolerates longer
  partitions. Engines before 1.2 do not know the directive and do not
  start with it, so leave it unset on them.
- **Forming the cluster.** Once every pod is ready, the operator
  introduces the nodes to each other (`CLUSTER MEET`), gives each shard's
  `-0` pod an even share of the slots, and has the shard's other pods
  `CLUSTER REPLICATE` it. Each node announces its pod's DNS name to clients
  (`cluster-announce-hostname`).
- **Failover is kividb's.** When a master fails, the engine promotes one of
  its replicas; the operator does not promote anything in cluster mode. It
  reports the move (`status.shards[].masterPod`, `lastFailoverTime`, a
  `ShardFailover` Event) and moves the `kividb.io/role` label, so
  `<cluster>-replicas` follows. From engine 1.2 the election favours the
  replica that has read the most of the master's stream: each replica
  waits an extra second for every other replica ahead of it
  (kividbio/kividb#107); before 1.2 any replica could win, and writes only
  another replica had were lost. Also from 1.2, a replica whose link to
  the master has been down for longer than `nodeTimeoutMs ×
  replicaValidityFactor` (by default 5 s × 10, about a minute) does not
  stand at all, so a shard whose replicas were all cut off that long stays
  down until its master is back (see `replicaValidityFactor` above). After
  a failover, the shard's other replicas continue from the new master's
  backlog rather than resyncing in full (1.2, kividbio/kividb#109).
- **Looking after it.** Every few seconds the operator re-introduces a node
  the others have at an old address (after a cold restart, every pod's IP
  changes), puts a pod that came back empty behind its shard's master, and,
  for a pod that rejoined with a new node ID because its volume was lost,
  forgets the old ID on every node (`status.cluster.staleNodeIDs` while it
  does). It never forgets a node that owns slots, nor one that belongs to a
  pod that exists, even when that pod is down, hung or its agent does not
  answer: a node is that pod's by the node ID the pod last reported
  (`status.pods[].nodeID`, kept while it cannot be asked), its IP or the
  name it announces. Only a pod that answers with a *different* node ID
  has its old one forgotten. A failed node that is no pod's at all (a
  replica removed by lowering `replicas`, or a forgotten node an old
  volume brought back) is forgotten once it has been so for 2 minutes.
- **Status.** `status.shards[]` (each shard's master, its slots, its pods),
  `status.cluster` (`cluster_state`, slots assigned and failed, known
  nodes), and each pod's shard and node ID. `Running` when every slot is
  served and every pod is ready; `Degraded` when the cluster serves but a
  pod is down, or `cluster_state` is `fail`.
- **Restarts.** A change to the pod template (image, resources, config,
  a new password) and a `KividbDbOps` restart replace the pods one at a
  time across the whole cluster, replicas first, and only while every
  slot is served and every other pod is ready and in sync; a wait is
  shown on the `RolloutProgressing` condition, and one of more than 2
  minutes by a `RolloutWaiting` Warning Event, as in a replicated
  cluster (see [Image and variant](#image-and-variant)). A shard's
  master is never restarted while it is the master: its most caught-up
  replica takes over first with `CLUSTER FAILOVER`, which the engine runs
  without losing a write, and the old master is restarted as a replica
  (a `ShardSwitchover` Event, not a `ShardFailover`). The same happens
  when a shard master's pod is deleted (by hand, a drain, an eviction):
  its `preStop` hook holds the engine while the replica takes over (see
  [Stopping a master](#switchovers-and-stopping-a-master)). A shard with
  `replicas: 0` has no one to hand over to: its master restarts in place,
  and its slots are down until it is back. A pod that has been unready
  for 30 s on an old template is replaced straight away.
  - **A force-killed master that is back within the node timeout** (5 s
    by default) keeps its slots: the engine never marked it failed, so no
    replica took over. It comes back from its last snapshot, and its
    replicas resync from it, dropping the writes they held that it lost
    (3 in the AKS test). The engine decides this in cluster mode; the
    operator does not hold a restarted master back, since delaying every
    master's start until its replicas fail over would also promote a
    replica holding less than the master's snapshot whenever both restart
    together (a cold restart). Kept as it is until the engine refuses such
    a resync.
  - **The handover pauses the shard's writes.** During `CLUSTER
    FAILOVER` the master holds its clients' writes until the replica has
    caught up and won its election: on engine 1.2 for up to 10 s. If the
    replica has not won within 5 s it gives the failover up (`Manual
    failover abandoned` in its log) and the master carries on once the
    pause ends; no write is lost either way (kividbio/kividb#185).
  - **When a handover does not happen,** the operator asks again 20 s
    after each request, at most 5 times in a row, then again after 10
    minutes. 15 s after a request that has not moved the master, it emits
    a `Warning` Event `ShardSwitchoverAbandoned`. While a handover is
    pending, the condition `ShardSwitchover` says which shard, which pods
    and which attempt (reason `InProgress`, `Retrying`, or `Stalled` once
    the attempts ran out); it goes when the master has moved. The rolling
    restart waits meanwhile. A `KividbDbOps` restart that has waited 5
    minutes for a master to hand over fails, says so in its status, and
    withdraws its step-down request.
- **Changing the shard count.** Edit `spec.shards` on a running cluster
  and the operator moves slots until they are spread evenly over the new
  count (phase `Rebalancing`, progress in `status.cluster.rebalance`):
  - **Adding shards:** the new shards' StatefulSets come up and join, the
    new pods sort into a master and replicas, and each existing shard
    gives the new ones its share. Going from 3 to 4 shards moves 4,096
    slots, a quarter, and nothing else.
  - **Removing shards:** the shards past the new count give all their
    slots to the others; then their StatefulSets, their volumes
    (PersistentVolumeClaims) and their nodes go. The volumes hold none of
    the cluster's data by then, only the nodes' identity and their view of
    the cluster, which would confuse a shard added later on them.
  - **How slots move:** one move at a time, each a `CLUSTER MIGRATION
    IMPORT` on the destination master. The engine copies the slots' keys,
    streams the writes made meanwhile, pauses writes for an instant and
    hands the slots over; clients never see an `ASK`. The source keeps
    serving the slots until then.
  - **Failures:** a move that fails leaves the slots with the source,
    and is tried again after 30 s (`status.cluster.rebalance.lastError`,
    a `SlotMoveFailed` Event). If the destination restarts during a move,
    engine 1.2 deletes the keys it had received for slots it does not
    own when it starts again. On 1.1.x they stay on it, unreachable
    (`MOVED`) but counted by `DBSIZE` and saved in its snapshots, until
    those slots are moved to it again (kividbio/kividb#108).
  - **Meanwhile:** rolling restarts wait until the change is done; a
    stuck pod is still replaced. Moves wait for every pod to be ready and
    the cluster to serve every slot. A change can be changed again half
    way: the shards of both counts are kept until the slots are where the
    last one says.
- **Not yet:** `bootstrapFromSnapshot`.

**Services**, all ClusterIP; the operator never creates a load balancer:

| Service | Selects | For |
|---|---|---|
| `<cluster>` | every pod | a seed address for cluster clients |
| `<cluster>-shard-<i>` | shard `i`'s pods | one shard |
| `<cluster>-replicas` | every replica | reads from replicas |
| `<pod>` (one per pod) | that pod (`statefulset.kubernetes.io/pod-name`) | a stable address per node |
| `<cluster>-<i>-headless` | shard `i`'s pods, not-ready included | each pod's DNS name, `<pod>.<cluster>-<i>-headless.<ns>.svc`; the only one with the agent port |

#### Access from outside Kubernetes

A cluster client connects to one address, then follows `MOVED` to the
address each node announces: by default the pod's DNS name, which only
resolves inside Kubernetes. To reach the cluster from outside, expose
**every pod** with a Service of your own and label it
`kividb.io/announce: "true"`; the node then announces that Service's
address instead.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: cache-0-0-public
  labels:
    kividb.io/announce: "true"
spec:
  type: LoadBalancer
  selector:
    statefulset.kubernetes.io/pod-name: cache-0-0
  ports:
    - port: 6380        # must be the cluster's client port
      targetPort: 6380
```

- **The address** is the load balancer's (`status.loadBalancer.ingress`,
  hostname first), or the `kividb.io/announce-address` annotation
  (`host` or `host:port`) for a Service without one of its own: a
  NodePort behind an address you know, an ingress controller's TCP route.
- **The port must be the client port.** A node announces one port, to
  clients and to the other nodes alike
  ([kividbio/kividb#110](https://github.com/kividbio/kividb/issues/110)),
  so a Service that serves another port, or an annotation that names one,
  is refused. Engine 1.2 adds `cluster-announce-client-port`, a port for
  clients only, which would lift this; the operator does not use it yet.
- **Hostnames and IPv4 addresses** only.
- **Everyone sees the outside address,** clients inside Kubernetes too,
  since a node announces one address. Inside clients then go through the
  load balancer. If you restrict it with `loadBalancerSourceRanges`, allow
  the pod network as well: kube-proxy applies the ranges to traffic from
  inside the cluster too, and inside clients would time out.
- **The operator never creates, changes or deletes these Services.** It
  reads them, and reconciles when one changes. Deleting one puts the
  pod's DNS name back.
- **Condition `ExternallyAccessible`**, once any pod is exposed:

  | Reason | Status | Means |
  |---|---|---|
  | `AllPodsExposed` | True | every pod announces an outside address |
  | `PartiallyExposed` | False | some pods are not exposed: outside clients are sent to every node and cannot reach those |
  | `WaitingForAddress` | False | a Service has no load-balancer address yet |
  | `AnnounceServiceProblem` | False | a Service serves the wrong port, its address cannot be announced, or two Services expose one pod |

### `podAnnotations` / `podLabels`

Merged onto every pod's metadata, in addition to the operator's own
`app.kubernetes.io/*`, `kividb.io/cluster` and `kividb.io/role` labels.

### Status fields (read-only, set by the operator)

```yaml
status:
  phase: Running   # Pending | Provisioning | Running | Degraded | FailingOver | Error
  masterPod: my-cluster-1
  pods:
    - name: my-cluster-0
      role: replica
      ready: true
      replicationOffset: 184320
    - name: my-cluster-1
      role: master
      ready: true
      replicationOffset: 184320
      replID: 9179439ba2951dfa54bd550abc5bc2fd1485bbac  # the master's stream
      runID: 3f1c...                                  # the engine's run_id
      incarnation: 6b0e.../0                          # pod UID / container restarts
  lastFailoverTime: "2026-06-12T03:14:22Z"
  observedGeneration: 7
  conditions:
    - type: Ready
      status: "True"
      reason: AllPodsReady
      message: "master=my-cluster-1, 3 pod(s) ready"
```

Backup outcomes are no longer summarized on `KividbCluster.status` — look
at the [`KividbSnapshot`](#kividbsnapshot-read-only) objects for a given
cluster instead (`kubectl get kdbs -l kividb.io/cluster=my-cluster`),
which carry per-run detail this single-cluster status block couldn't.

## KividbConfig

```yaml
apiVersion: kividb.io/v1alpha1
kind: KividbConfig
metadata:
  name: my-cluster-config
spec:
  directives:
    maxmemory: "1073741824"        # bytes
    threads: "4"
    aof: "yes"
    loglevel: "notice"
    notify-keyspace-events: "Ex"
    slowlog-log-slower-than: "10000"
  tls:
    enabled: true
    port: 6443                      # default shown
    certSecretRef:
      name: my-cluster-tls
      certKey: tls.crt              # default shown
      keyKey: tls.key                # default shown
      caKey: ca.crt                  # default shown; omit the key from the Secret to skip client-cert verification
```

- `directives`: any directive kividb's `--configfile` accepts (see
  kividb's own `COMMANDS.md`/`--help` for the full list — this operator
  does not maintain a duplicate list and does not validate directive
  *names*, only that you don't set the operator-owned ones below). Values
  are always strings; kividb parses them itself.

  **A directive the engine refuses stops the server at startup.** Since
  1.0.5 an unknown name or a value that does not parse is fatal (`Bad
  directive or wrong number of arguments`, exit 1). Engine 1.2 accepts
  Redis's own settings that mean nothing to KiviDB with a warning, so a
  carried-over `redis.conf` starts, and now applies settings earlier
  versions ignored in the file; a name that is neither KiviDB's nor
  Redis's, or a bad value, still stops it. Check a `KividbConfig` against
  the engine version before you apply it:
  - **On a running cluster,** the change rolls out one pod at a time,
    replicas first, and a pod that crash-loops is not ready, so nothing
    else is replaced: the rollout stalls after the first pod and the
    cluster keeps serving. Fix or revert the `KividbConfig` and the
    stuck pod is replaced. A pod that restarts on its own in the meantime
    reads the new file too (the config is a mounted Secret, updated in
    place), and crash-loops the same way.
  - **On a new cluster, or a restart of every pod** (a cold restart after
    the nodes went away), every pod reads the file at once and every pod
    is down until the directive is fixed.

  **Do not set** `replicaof`, `port`, or `aclfile` — they are overwritten
  by the operator regardless of what's in `directives` (replication
  topology is managed dynamically at runtime, not via the config file;
  `port` and `aclfile` are pinned so the operator's own probes/mounts stay
  correct).

  **`bind`**: unless you set `bind` or `protected-mode` yourself, the
  operator sets `bind 0.0.0.0`, the address kividb has always listened on.
  kividb v1.1.0 and later enforce protected mode as Redis does: while the
  `default` user has no password and no `bind` was configured, they refuse
  every client that is not on the pod's own loopback, other pods' replication
  included. An explicit `bind` turns that off, so a cluster without a
  password stays reachable inside Kubernetes, as it was on earlier engines.
  Protect such a cluster with a password (`aclConfigRef`) and a
  `NetworkPolicy`.

- `tls`: requires the referencing `KividbCluster`'s `spec.variant` to be
  `tls` or `full` — see [Variants](#variants). `certSecretRef` names a
  Secret matching the standard `kubernetes.io/tls` Secret's default key
  layout, so a cert-manager-issued `Certificate`'s Secret can usually be
  referenced directly without relabeling keys.

  With a CA (`caKey`), kividb v1.1.0 and later **require a client
  certificate** signed by that CA, as Redis does; earlier engines accepted
  TLS clients without one. To accept clients without a certificate, add the
  directive `tls-auth-clients: optional` (verify a certificate when one is
  sent) or `"no"` (never ask). The Services carry the TLS port too (named
  `tls`; see [`services`](#services--master-and-replica-exposure)).

A `KividbConfig` is standalone: create it once, reference it from as many
`KividbCluster`s as share the same tuning/TLS needs via `spec.configRef`.
Deleting one while a cluster still references it surfaces as a reconcile
error on that cluster (see [ROADMAP.md](ROADMAP.md) for planned
usage-tracking to catch this earlier).

## KividbAclConfig

> **On kividb before v1.0.5 the `default` user's password does not keep
> unauthenticated clients out.** A client that never sends `AUTH` is
> treated as `default` (see [KIVIDB_ENGINE_ISSUES.md](KIVIDB_ENGINE_ISSUES.md),
> issue 1). v1.0.5 and later enforce it, including v1.1.2, the default image of this release. If you
> pin an older engine with `spec.image`, limit who can reach the cluster's
> Services with a NetworkPolicy.

```yaml
apiVersion: kividb.io/v1alpha1
kind: KividbAclConfig
metadata:
  name: my-cluster-acl
spec:
  requirePassSecretRef:
    name: my-cluster-default-password
    key: password
  users:
    - name: app
      passwordSecretRef:
        name: my-cluster-app-password
        key: password
      keyPatterns: ["~cache:*", "~session:*"]
      channelPatterns: ["&*"]
      commandRules: ["+@all", "-flushall", "-flushdb", "-config"]
    - name: readonly
      noPass: false
      passwordSecretRef:
        name: my-cluster-readonly-password
        key: password
      commandRules: ["+@read"]
```

- If no `users` entry is named `default`, one is synthesized: it uses
  `requirePassSecretRef` if set, otherwise it's `nopass` (open access).
- Every user needs either `noPass: true` or a `passwordSecretRef`.
- Defaults when a user's field is omitted: `keyPatterns: ["~*"]`,
  `channelPatterns: ["&*"]`, `commandRules: ["+@all"]`, `enabled: true`.
- Passwords are hashed (SHA-256, matching kividb's own ACL storage) into
  the operator-managed `<cluster>-auth` Secret; the plaintext only ever
  lives in the Secret(s) you provide via `passwordSecretRef`.
- The `agent` sidecar authenticates its own local RESP connections using
  the **default** user's password (same secret reference), so it can
  still run `PING`/`INFO`/`REPLICAOF`/backups after you turn on auth.
- Referenced the same way as `KividbConfig`: create once, point any
  number of `KividbCluster`s at it via `spec.aclConfigRef`.

### Changing the default user's password

The pods read the default user's password at startup (the agent, the
exporter and, in a sharded cluster, `masterauth`), so a change, a new
password or its removal, restarts them one at a time. So that neither a
client nor a replica is refused meanwhile, both passwords are accepted
until every pod runs with the new one:

1. Before any pod restarts, the pods still on the previous password are
   given the new one live: `ACL SETUSER default #<hash>` adds it next to
   the old one; for a removal, `ACL SETUSER default nopass`. Each is
   annotated `kividb.io/auth-widened: <generation>`. If one cannot be
   (`PasswordOverlapFailed` Warning), the rollout waits.
2. Pods started during the rollout get an ACL file that accepts both
   passwords (or none, when either side has none: a user without a
   password accepts any).
3. When every pod runs with the new password, the ACL file is rendered
   with it alone and every pod reloads it (`PasswordChanged` Event); the
   previous password stops working only then, as soon as the pods have the
   updated file (the kubelet takes up to a minute or two to refresh a
   Secret volume).

Clients can therefore move to the new password at any time after the
operator has seen the change (it reconciles every 10 s). Setting a
password on a cluster that had none keeps it open until the rollout is
over. If the rollout cannot finish, the previous password stays valid.

> **Live-tested caveat:** kividb currently accepts and correctly reports
> (via `ACL GETUSER`) negative `commandRules` entries like `-flushall`,
> but does not actually enforce them — a user configured with
> `+@all -flushall` can still run `FLUSHALL`. Don't rely on negative
> command rules as a security boundary until this is fixed upstream. See
> [ROADMAP.md](ROADMAP.md#known-upstream-kividb-issues-confirmed-by-live-testing-2026-07-23).

## KividbSnapshotConfig

```yaml
apiVersion: kividb.io/v1alpha1
kind: KividbSnapshotConfig
metadata:
  name: my-cluster-backups
spec:
  schedule: "0 * * * *"        # standard cron, hourly
  retention: 24                 # keep the last 24 snapshots; 0 = keep all
  source: master                 # default shown, and recommended -- see below
  timeoutSeconds: 900            # bounds the whole backup+upload run
  s3:
    endpoint: "https://s3.us-east-1.amazonaws.com"
    bucket: "my-kividb-backups"
    region: "us-east-1"
    pathPrefix: "prod"
    forcePathStyle: false        # set true for most self-hosted MinIO
    insecureSkipTLSVerify: false # dev/test self-signed MinIO only
    credentialsSecretRef:
      name: my-cluster-s3-creds
      accessKeyIdKey: accessKeyId        # default shown
      secretAccessKeyKey: secretAccessKey # default shown
  jobResources:
    requests: {cpu: "50m", memory: "64Mi"}
    limits: {cpu: "250m", memory: "256Mi"}
```

`schedule` and `s3` are required. Any S3-compatible endpoint works: AWS
S3, MinIO, Ceph RGW, Backblaze B2, Cloudflare R2, etc.

### `source`: master or replica

`source` picks which role's pod the scheduled backup Job targets:

- **`master` (default, recommended).** The master always holds the most
  complete, up-to-date data. kividb's own replication currently has no
  guaranteed bound on replica lag, so a replica-sourced snapshot can be
  measurably stale in a way that's hard to predict from outside.
- **`replica`.** Trades that small, variable staleness window for zero
  `BGSAVE` I/O impact on the pod actually serving writes — worth it if
  your workload is write-latency-sensitive and you can tolerate backups
  that lag by an unknown amount. Requires at least one replica (`spec.replicas
  >= 1` on the `KividbCluster`); if none is ready when the schedule fires,
  the run fails rather than silently falling back to the master.

Each scheduled run creates a [`KividbSnapshot`](#kividbsnapshot-read-only)
recording which pod and role it actually used, so you can audit this after
the fact even if `source` or the cluster's replica count changes over
time.

A `KividbSnapshotConfig` is standalone: create once, reference from any
number of `KividbCluster`s via `spec.snapshotConfigRef`. See
[BACKUP_RESTORE.md](BACKUP_RESTORE.md) for the manual restore procedure
and how to trigger an out-of-schedule backup.

## KividbSnapshot (read-only)

```yaml
apiVersion: kividb.io/v1alpha1
kind: KividbSnapshot
metadata:
  name: my-cluster-backups-20260721t000004z
spec:
  clusterRef:
    name: my-cluster
  snapshotConfigRef:
    name: my-cluster-backups
status:
  phase: Succeeded              # Pending | InProgress | Succeeded | Failed
  startTime: "2026-07-21T00:00:04Z"
  completionTime: "2026-07-21T00:00:41Z"
  sourcePod: my-cluster-1
  sourceRole: master
  objectKey: "prod/my-cluster/my-cluster-1-20260721T000004Z.tar.gz"
  sizeBytes: 483920104
  durationMs: 37211
  error: ""                      # populated only when phase is Failed
```

For a sharded cluster, `objectKey` is the run's `manifest.json` and
`status.shards[]` lists each shard's `index`, `sourcePod`, `sizeBytes`
and, on failure, `error`. See
[BACKUP_RESTORE.md](BACKUP_RESTORE.md#sharded-clusters).

You don't create these — the operator creates exactly one per backup Job
run, named after the owning `KividbSnapshotConfig` and timestamp. List a
cluster's backup history with:

```sh
kubectl get kdbs -l kividb.io/cluster=my-cluster --sort-by=.status.startTime
```

`status` is a genuine subresource (`kubectl edit` on `.spec` won't touch
it) — the operator populates it by reading the backup Job's termination
message, not by scraping logs, so it's available even if you're not
collecting pod logs anywhere. See [ARCHITECTURE.md](ARCHITECTURE.md) for
how that plumbing works.
