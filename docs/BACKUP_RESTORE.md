# Backups and restore

## How scheduled backups work

See [ARCHITECTURE.md](ARCHITECTURE.md#backups) for the full design. In
short: a `KividbCluster` with `spec.snapshotConfigRef` set to a
[`KividbSnapshotConfig`](CONFIGURATION.md#kividbsnapshotconfig) gets a
Kubernetes `CronJob` named `<cluster-name>-backup`, running on that
`KividbSnapshotConfig`'s `spec.schedule`. Its pod runs one command:

```
agent backup-trigger --targets <pod>.<cluster>-headless.<namespace>.svc:8081,... --role master --timeout <spec.timeoutSeconds>s
```

It asks every pod's agent for its role and backs up from the first holding
`--role`: `master` or `replica`, depending on the `KividbSnapshotConfig`'s
`spec.source` (`master` by
default and recommended — see
[CONFIGURATION.md](CONFIGURATION.md#source-master-or-replica) for the
trade-off). Whichever pod that Service resolves to does the actual work
via its own `agent` sidecar: `BGSAVE`, wait for `LASTSAVE` to advance,
`tar.gz` `dump.kdb` (+ `appendonly.aof`, which the operator turns on by
default from 0.7.0, see
[CONFIGURATION.md](CONFIGURATION.md#persistence-the-append-only-file)), stream it
straight into your S3-compatible bucket, then delete the cluster's own
archives beyond `spec.retention`. Only its pods'
`<pod-name>-<timestamp>.tar.gz` objects directly under its folder count;
anything else there is left alone.

Objects are stored at:

```
s3://<bucket>/<pathPrefix>/<namespace>.<cluster-name>/<pod-name>-<UTC timestamp>.tar.gz
```

e.g. `s3://my-kividb-backups/prod/default.my-cluster/my-cluster-1-20260721T000004Z.tar.gz`.
Clusters of the same name in different namespaces can share a bucket and
`pathPrefix`: neither writes to or prunes the other's folder, nor any
folder of the older layout. Clusters in different Kubernetes clusters
cannot: give each its own bucket or `pathPrefix`.

An agent from 0.6.x or earlier (a pod not rolled yet, or a
`spec.agentImage` pinned to it) deletes anything under its
`<pathPrefix>/<cluster-name>/`. That folder never holds new keys of a
cluster with the same `pathPrefix`, but it does hold those of a cluster
whose `pathPrefix` is `<pathPrefix>/<cluster-name>` or lies under it, as it always
did. Upgrading that agent ends it.

**Upgrading from 0.6.x or earlier:** backups taken before this release stay
at `<pathPrefix>/<cluster-name>/<cluster-name>-<ordinal>-<UTC timestamp>.tar.gz`
(sharded runs from pre-release builds: `<pathPrefix>/<cluster-name>/<run>/`).
Their `KividbSnapshot`s still restore, since they record the full key.
Retention no longer deletes them: a same-named cluster in another namespace
may have written them too, and nothing in the key tells them apart. Delete
them by hand once newer backups hold what you want to keep. If a
same-named cluster in another namespace shares the `pathPrefix`, first
check which old keys its `KividbSnapshot`s still name:

```bash
kubectl get kdbs -A -o custom-columns=NS:.metadata.namespace,KEY:.status.objectKey
```

Every run — success or failure — produces a
[`KividbSnapshot`](CONFIGURATION.md#kividbsnapshot-read-only) object
recording exactly which pod/role was used, the object key, size, and
duration. You don't need to scrape Job logs to find any of this.

### Sharded clusters

A sharded cluster's backup is one **run** across every shard. The Job
runs `agent backup-trigger --shards <shard 0's agents>;<shard 1's
agents>;... --cluster <name> --role <source>`, finds a pod with the
wanted role in each shard, and has all shards upload at once:

```
s3://<bucket>/<pathPrefix>/<namespace>.<cluster-name>/<run>/shard-<i>-<pod>.tar.gz
s3://<bucket>/<pathPrefix>/<namespace>.<cluster-name>/<run>/manifest.json
```

`<run>` is the UTC time the run started (`20261025T101500Z`). Once every
shard's object is there, shard 0's agent writes `manifest.json`: for each
shard, its object, the pod and role it came from, and the master's node
ID and slots when it was taken. **A run without a manifest is
incomplete**, because a shard failed. The `KividbSnapshot` is `Failed`
with the shard's error, and the run is pruned with the old ones.

- **Retention counts runs**, not objects: `retention: 24` keeps the 24
  newest complete runs, every shard's object in them, and deletes older
  runs whole.
- **Not one instant:** each shard's snapshot is taken when its `BGSAVE`
  runs, a moment apart from the others. A write that spans shards (the
  engine refuses multi-key commands across slots anyway) can be in one
  shard's snapshot and not another's.
- **`source: replica`** needs a replica in every shard.
- The `KividbSnapshot` has `status.objectKey` set to the manifest and
  `status.shards[]` (each shard's source pod and size).
- **Restoring** a sharded cluster (`bootstrapFromSnapshot`) is not
  supported yet. A manifest has what a restore needs. A replicated
  cluster whose `bootstrapFromSnapshot` names a sharded run's
  `KividbSnapshot` is refused before any restore Job runs
  (`status.bootstrap.phase: Failed`, a `BootstrapRefused` Event): one
  shard's archive holds only that shard's slots. To get at one shard's
  data, restore its archive by hand (see [Manual restore](#manual-restore-legacy--emergency)).

## Configuring backups

Create a `KividbSnapshotConfig`:

```yaml
apiVersion: kividb.io/v1alpha1
kind: KividbSnapshotConfig
metadata:
  name: my-cluster-backups
spec:
  schedule: "0 * * * *"
  retention: 24
  source: master
  s3:
    endpoint: "https://s3.us-east-1.amazonaws.com"
    bucket: "my-kividb-backups"
    region: "us-east-1"
    pathPrefix: "prod"
    credentialsSecretRef:
      name: my-cluster-s3-creds
```

Then reference it from the cluster:

```yaml
apiVersion: kividb.io/v1alpha1
kind: KividbCluster
metadata:
  name: my-cluster
spec:
  # ...
  snapshotConfigRef:
    name: my-cluster-backups
```

Create the credentials Secret yourself (the operator never generates S3
credentials):

```bash
kubectl create secret generic my-cluster-s3-creds \
  --from-literal=accessKeyId=AKIA... \
  --from-literal=secretAccessKey='...'
```

Key names default to `accessKeyId`/`secretAccessKey`; override via
`s3.credentialsSecretRef.accessKeyIdKey`/`secretAccessKeyKey` if your
Secret uses different keys.

For self-hosted MinIO, also set `forcePathStyle: true` (most MinIO
deployments need path-style addressing) and, for self-signed dev/test
instances only, `insecureSkipTLSVerify: true`.

A single `KividbSnapshotConfig` can be referenced by more than one
`KividbCluster` — useful when several clusters should share one bucket
and schedule; each cluster still gets its own `KividbSnapshot` records
and its own CronJob.

## Triggering a backup on demand

The CronJob is just a thin trigger — you can do the same HTTP call
yourself at any time, from inside the cluster:

```bash
kubectl create job backup-now --from=cronjob/my-cluster-backup
```

(The agents need the agent token, which the CronJob's pod takes from the
cluster's auth Secret; a pod started by hand would have to be given it
too.)

Or trigger the existing CronJob's Job directly:

```bash
kubectl create job my-cluster-backups-manual --from=cronjob/my-cluster-backup
kubectl get kdbs -l kividb.io/cluster=my-cluster --sort-by=.status.startTime
```

Either way, the next reconcile picks up the resulting Job/pod and creates
a `KividbSnapshot` for it exactly as it would for a scheduled run — there
is no separate "manual backup" record type.

## Checking backup status

```bash
kubectl get kdbs -l kividb.io/cluster=my-cluster --sort-by=.status.startTime
```

```
NAME                                      CLUSTER      PHASE       OBJECT KEY
my-cluster-backups-20260720t230004z       my-cluster   Succeeded   prod/default.my-cluster/my-cluster-1-20260720T230004Z.tar.gz
my-cluster-backups-20260721t000004z       my-cluster   Succeeded   prod/default.my-cluster/my-cluster-1-20260721T000004Z.tar.gz
```

For full detail on the most recent run:

```bash
kubectl get kdbs my-cluster-backups-20260721t000004z -o yaml
```

`status.phase` is `Pending` → `InProgress` → `Succeeded`/`Failed`.
`status.error` is populated only when `phase: Failed` — check it first,
then `kubectl get jobs -l kividb.io/cluster=my-cluster` and `kubectl logs`
on the most recent Job pod for the underlying cause (auth failure, wrong
bucket, network egress blocked, etc.) if `status.error` alone isn't
enough.

## Restoring a backup

### Supported: bootstrap a **new** cluster from a snapshot (0.4.0+)

Create a new `KividbCluster` with `spec.bootstrapFromSnapshot` pointing at a
`Succeeded` `KividbSnapshot` in the same namespace. The operator seeds
pod-0's PVC from S3 before starting the StatefulSet; replicas then
full-sync via the normal `REPLICAOF` path.

```yaml
apiVersion: kividb.io/v1alpha1
kind: KividbCluster
metadata:
  name: my-cluster-restored
spec:
  image: docker.io/kividb/kividb:v1.2.1
  replicas: 1
  storage:
    size: 5Gi
  bootstrapFromSnapshot:
    snapshotRef:
      name: my-cluster-backups-20260721t000004z
```

Watch progress on `status.bootstrap` and the `Bootstrapped` condition:
`False` until the restore Job succeeds, with a reason that says what the
bootstrap waits for or does (`WaitingForEarlierCluster`,
`WaitingForVolumeDeletion`, `Restoring`, `RestoreRetrying`,
`RestoreFailed`), then `True`. A restore pod that fails is retried once:
the bootstrap stays `InProgress`, its message says which attempt failed
and why, with a `BootstrapRetrying` Event, and it is `Failed` (a
`BootstrapFailed` Event) only once the Job has given up.
Once `status.bootstrap.completed` is true, the
field is ignored on later reconciles (immutable bootstrap). The field can
only be set when the cluster is created: the API server refuses it on an
existing cluster, and a cluster that is already running skips it with a
`BootstrapSkipped` Event. If a StatefulSet with the cluster's name, its
pods, or a `<cluster>-bootstrap` restore Job are left from an earlier
cluster (still being garbage-collected, or orphaned with
`--cascade=orphan`), the bootstrap waits in `Pending` with a
`BootstrapWaiting` Event and starts once they are deleted (with
`--cascade=orphan`, delete the pods and the Job too). Removing
`spec.bootstrapFromSnapshot` while it waits cancels the bootstrap. A
`data-<cluster>-0` PVC that is still being deleted (held by a finalizer,
say) is waited for too: `status.bootstrap.message` names it, with a
`BootstrapWaitingForVolume` Event.

**Re-creating a deleted cluster with the same name.** The StatefulSet
keeps its PVCs when the cluster is deleted, so the new cluster finds
`data-<cluster>-0` (and `data-<cluster>-1` and up) with the earlier
cluster's data. The restore replaces pod-0's data with the snapshot's,
as the spec asks; `status.bootstrap.message` says it restored over an
existing volume older than the cluster, with a
`BootstrapOverExistingVolume` Warning Event. The replicas' PVCs are not
touched: the first election makes the restored pod-0 the master, and
the other pods are made its replicas and resync from it. For a clean
start, delete the earlier cluster's `data-<cluster>-1` and up (or all
its PVCs) before re-creating the cluster.

This is **not** an in-place restore of a live cluster — that remains out of
scope (see [ROADMAP.md](ROADMAP.md)).

### Manual restore (legacy / emergency)

If you need to rewrite an existing PVC by hand:

1. **Find the object key** from a `KividbSnapshot`'s `status.objectKey`,
   then download and extract it locally:

   ```bash
   aws s3 cp s3://my-kividb-backups/prod/default.my-cluster/my-cluster-1-20260721T000004Z.tar.gz .
   tar xzf my-cluster-1-20260721T000004Z.tar.gz   # produces dump.kdb (and appendonly.aof)
   ```

2. **Scale the StatefulSet to 0**:

   ```bash
   kubectl scale statefulset my-cluster --replicas=0
   ```

3. **Copy the files onto the PVC** (short-lived debug pod mounting
   `data-<sts>-0`): both `dump.kdb` and `appendonly.aof`. First delete the
   volume's own engine files, all but `dump.kdb`: `appendonly.aof` and every
   `appendonly.aof.*` (`.prev`, `.prev.len`, `.rewrite.tmp`, `.tmp`), every
   `dump.kdb.*` (`dump.kdb.gen`, `dump.kdb.repl`, `dump.kdb.clean` and temp
   files), every `superseded.*` (logs engine 1.2.1 kept aside) and
   `dump.rdb`. They belong to the data being replaced: a stale
   `dump.kdb.gen` makes the engine refuse to start, or skip the archive's
   `appendonly.aof`; without `dump.kdb.gen`, a stale `appendonly.aof` (one
   the archive does not replace, or one still there when the restored dump
   is in place) or `appendonly.aof.prev` is replayed on top of the restored
   dump; and the engine names each `superseded.*` file at every start, with
   how to apply its writes. The restore Job removes the same files.

   The archive's `appendonly.aof` holds the writes made while the snapshot was
   taken. The engine replays it only if it runs with the append-only file on
   (the operator's default, or `--aof`); with it off, the engine loads
   `dump.kdb` alone, and those writes are missing.

4. **Scale back up** to `replicas+1` and verify with
   `kubectl get kividbcluster my-cluster`.

Prefer `bootstrapFromSnapshot` for recovery onto a fresh cluster name.
