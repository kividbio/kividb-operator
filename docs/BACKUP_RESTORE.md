# Backups and restore

## How scheduled backups work

See [ARCHITECTURE.md](ARCHITECTURE.md#backups) for the full design. In
short: a `KividbCluster` with `spec.snapshotConfigRef` set to a
[`KividbSnapshotConfig`](CONFIGURATION.md#kividbsnapshotconfig) gets a
Kubernetes `CronJob` named `<snapshotconfig-name>-backup`, running on that
`KividbSnapshotConfig`'s `spec.schedule`. Its pod runs one command:

```
agent backup-trigger --url http://<source-service>.<namespace>.svc:8081/backup --timeout <spec.timeoutSeconds>s
```

`<source-service>` is `<cluster>-master` or `<cluster>-replicas`,
depending on the `KividbSnapshotConfig`'s `spec.source` (`master` by
default and recommended — see
[CONFIGURATION.md](CONFIGURATION.md#source-master-or-replica) for the
trade-off). Whichever pod that Service resolves to does the actual work
via its own `agent` sidecar: `BGSAVE`, wait for `LASTSAVE` to advance,
`tar.gz` `dump.kdb` (+ `appendonly.aof` if AOF is enabled), stream it
straight into your S3-compatible bucket, then delete objects beyond
`spec.retention`.

Objects are stored at:

```
s3://<bucket>/<pathPrefix>/<cluster-name>/<pod-name>-<UTC timestamp>.tar.gz
```

e.g. `s3://my-kividb-backups/prod/my-cluster/my-cluster-1-20260721T000004Z.tar.gz`.

Every run — success or failure — produces a
[`KividbSnapshot`](CONFIGURATION.md#kividbsnapshot-read-only) object
recording exactly which pod/role was used, the object key, size, and
duration. You don't need to scrape Job logs to find any of this.

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
kubectl run backup-now --rm -i --restart=Never \
  --image=quay.io/kividbio/kividb-operator-agent:latest -- \
  backup-trigger --url http://my-cluster-master.default.svc:8081/backup --timeout 900s
```

Or trigger the existing CronJob's Job directly:

```bash
kubectl create job my-cluster-backups-manual --from=cronjob/my-cluster-backups-backup
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
my-cluster-backups-20260720t230004z       my-cluster   Succeeded   prod/my-cluster/my-cluster-1-20260720T230004Z.tar.gz
my-cluster-backups-20260721t000004z       my-cluster   Succeeded   prod/my-cluster/my-cluster-1-20260721T000004Z.tar.gz
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
  image: quay.io/kividbio/kividb:v1.0.5
  replicas: 1
  storage:
    size: 5Gi
  bootstrapFromSnapshot:
    snapshotRef:
      name: my-cluster-backups-20260721t000004z
```

Watch progress on `status.bootstrap` / cluster conditions (`Bootstrapping`
until the Job finishes). Once `status.bootstrap.completed` is true, the
field is ignored on later reconciles (immutable bootstrap).

This is **not** an in-place restore of a live cluster — that remains out of
scope (see [ROADMAP.md](ROADMAP.md)).

### Manual restore (legacy / emergency)

If you need to rewrite an existing PVC by hand:

1. **Find the object key** from a `KividbSnapshot`'s `status.objectKey`,
   then download and extract it locally:

   ```bash
   aws s3 cp s3://my-kividb-backups/prod/my-cluster/my-cluster-1-20260721T000004Z.tar.gz .
   tar xzf my-cluster-1-20260721T000004Z.tar.gz   # produces dump.kdb (and appendonly.aof)
   ```

2. **Scale the StatefulSet to 0**:

   ```bash
   kubectl scale statefulset my-cluster --replicas=0
   ```

3. **Copy the files onto the PVC** (short-lived debug pod mounting
   `data-<sts>-0`).

4. **Scale back up** to `replicas+1` and verify with
   `kubectl get kividbcluster my-cluster`.

Prefer `bootstrapFromSnapshot` for recovery onto a fresh cluster name.
