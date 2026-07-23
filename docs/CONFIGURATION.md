# KividbCluster field reference

Group/version: `kividb.io/v1alpha1`. Kind: `KividbCluster`. Short names:
`kdb`, `kdbc`. All fields below are under `spec:` unless stated otherwise.
See [config/samples/](../config/samples/) for complete, applyable examples.

## Core

| Field | Type | Default | Notes |
|---|---|---|---|
| `replicas` | int32 | `2` | Replica pods **in addition to** the single master. Total pods = `replicas + 1`. |
| `image` | string | *(required)* | kividb container image, e.g. `quay.io/kividbio/kividb:v1.0.2`. |
| `imagePullPolicy` | string | `IfNotPresent` | `Always`, `IfNotPresent`, or `Never`. |
| `imagePullSecrets` | `[]LocalObjectReference` | — | Standard Kubernetes image pull secrets. |
| `agentImage` | string | the operator's bundled agent image | Override only if you build your own agent image. |
| `exporterImage` | string | `oliver006/redis_exporter:v1.66.0` | Only used when `monitoring.enabled: true`. See [`monitoring`](#monitoring). |
| `port` | int32 | `6380` | RESP port. Also written into the rendered `kividb.conf` as `port`. |
| `terminationGracePeriodSeconds` | int64 | `60` | kividb snapshots on SIGTERM; keep this comfortably above your data size's `SAVE` time. |

## `kividbConfig` — free-form kividb.conf directives

```yaml
spec:
  kividbConfig:
    maxmemory: "1073741824"        # bytes
    threads: "4"
    aof: "yes"
    loglevel: "notice"
    cluster-enabled: "no"
    notify-keyspace-events: "Ex"
    slowlog-log-slower-than: "10000"
```

Any directive kividb's `--configfile` accepts can go here (see kividb's
own `COMMANDS.md`/`--help` for the full list — this operator does not
maintain a duplicate list and does not validate directive *names*, only
that you don't set the three operator-owned ones below). Values are
always strings; kividb parses them itself.

**Do not set** `replicaof`, `port`, or `aclfile` here — they are
overwritten by the operator (replication topology is managed dynamically
at runtime, not via the config file; `port` and `aclfile` are pinned so
the operator's own probes/mounts stay correct).

## `auth` — requirepass and ACL users

```yaml
spec:
  auth:
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
  `requirePassSecretRef` if set, otherwise it's `nopass` (open access —
  fine for local/dev, not for anything reachable outside the cluster).
- Every user needs either `noPass: true` or a `passwordSecretRef`.
- Defaults when omitted: `keyPatterns: ["~*"]`, `channelPatterns: ["&*"]`,
  `commandRules: ["+@all"]`, `enabled: true`.
- Passwords are hashed (SHA-256, matching kividb's own ACL storage) into
  the operator-managed `<cluster>-auth` Secret; the plaintext only ever
  lives in the Secret(s) you provide via `passwordSecretRef`.
- The `agent` sidecar authenticates its own local RESP connections using
  the **default** user's password (same secret reference), so it can
  still run `PING`/`INFO`/`REPLICAOF`/backups after you turn on auth.

## `storage`

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

## Scheduling: `tolerations`, `nodeSelector`, `affinity`

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

## `resources` / `agentResources` / `exporterResources`

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

## `services` — master and replica exposure

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

## `backup` — scheduled S3 snapshots

```yaml
spec:
  backup:
    enabled: true
    schedule: "0 * * * *"        # standard cron, hourly
    retention: 24                 # keep the last 24 snapshots; 0 = keep all
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

`schedule` and `s3` are required when `enabled: true`. Any S3-compatible
endpoint works: AWS S3, MinIO, Ceph RGW, Backblaze B2, Cloudflare R2, etc.
See [BACKUP_RESTORE.md](BACKUP_RESTORE.md) for the manual restore
procedure and how to trigger an out-of-schedule backup.

## `monitoring`

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

## `failover`

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

## `podAnnotations` / `podLabels`

Merged onto every pod's metadata, in addition to the operator's own
`app.kubernetes.io/*`, `kividb.io/cluster` and `kividb.io/role` labels.

## Status fields (read-only, set by the operator)

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
  backup:
    lastRunTime: "2026-07-21T00:00:04Z"
    lastSuccessTime: "2026-07-21T00:00:41Z"
    lastObjectKey: "prod/my-cluster/my-cluster-1-20260721T000004Z.tar.gz"
  observedGeneration: 7
  conditions:
    - type: Ready
      status: "True"
      reason: AllPodsReady
      message: "master=my-cluster-1, 3 pod(s) ready"
```
