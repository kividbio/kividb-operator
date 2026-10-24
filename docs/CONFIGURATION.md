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
| `terminationGracePeriodSeconds` | int64 | `60` | kividb snapshots on SIGTERM; keep this comfortably above your data size's `SAVE` time. |

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
is first handed to an in-sync replica (a `Switchover` event; expect about
a second of failed writes) and the old master is replaced afterwards. A pod that is unready and still on the
old template is replaced without waiting, so a change that fixes a
crash-looping pod is not held up by that pod. Changes to a `KividbAclConfig` are
applied without a restart unless the `default` user's password changed —
see [TROUBLESHOOTING.md](TROUBLESHOOTING.md#acl--authentication-errors-noauth-wrongpass).

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

> **TLS on kividb v1.0.3+:** e2e against `v1.0.3-tls` / `v1.0.3-full`
> confirmed the TLS port is in `LISTEN` when the operator mounts certs
> and passes CLI flags (see `hack/e2e/04-compat-variants.sh`). On
> **v1.0.2** the listener never opened — pin `spec.image` to v1.0.3+ if
> you need TLS. The operator still writes both conf directives and CLI
> flags because `--configfile` historically dropped `tls-*` keys; track
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
`BGSAVE`, a brief network hiccup).

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
    # busSecretRef: {name: my-bus, key: secret}   # generated into <cluster>-bus when unset
```

- **Fixed at creation.** A cluster is sharded, or not, for good:
  `spec.shards` cannot be added to or removed from an existing cluster.
- **At most 24 nodes** (`shards × (replicas + 1)`) for now, and a name of
  at most 40 characters.
- **Workloads.** One StatefulSet per shard, `<cluster>-<shard>`, whose pods
  are `<cluster>-<shard>-<n>`, and a PodDisruptionBudget per shard
  (`maxUnavailable: 1`). Pods prefer other nodes than their shard's other
  pods, then than the cluster's.
- **Configuration.** The operator sets every cluster-mode directive
  (`cluster-*`, `min-replicas-*`, `masteruser`/`masterauth`, `replicaof`,
  `tls-cluster`); a `KividbConfig` that sets one is refused. The bus
  secret is always on.
- **Forming the cluster.** Once every pod is ready, the operator
  introduces the nodes to each other (`CLUSTER MEET`), gives each shard's
  `-0` pod an even share of the slots, and has the shard's other pods
  `CLUSTER REPLICATE` it. Each node announces its pod's DNS name to clients
  (`cluster-announce-hostname`).
- **Failover is kividb's.** When a master fails, the engine promotes one of
  its replicas; the operator does not promote anything in cluster mode. It
  reports the move (`status.shards[].masterPod`, `lastFailoverTime`, a
  `ShardFailover` Event) and moves the `kividb.io/role` label, so
  `<cluster>-replicas` follows.
- **Looking after it.** Every few seconds the operator re-introduces a node
  the others have at an old address (after a cold restart, every pod's IP
  changes), puts a pod that came back empty behind its shard's master, and,
  for a pod that rejoined with a new node ID because its volume was lost,
  forgets the old ID on every node (`status.cluster.staleNodeIDs` while it
  does). It never forgets a node that owns slots.
- **Status.** `status.shards[]` (each shard's master, its slots, its pods),
  `status.cluster` (`cluster_state`, slots assigned and failed, known
  nodes), and each pod's shard and node ID. `Running` when every slot is
  served and every pod is ready; `Degraded` when the cluster serves but a
  pod is down, or `cluster_state` is `fail`.
- **Not yet:** `bootstrapFromSnapshot`, and `KividbDbOps` restarts.

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
  is refused.
- **Hostnames and IPv4 addresses** only.
- **Everyone sees the outside address,** clients inside Kubernetes too,
  since a node announces one address. Inside clients then go through the
  load balancer.
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
