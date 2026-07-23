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

**There is no automated restore yet** (see [ROADMAP.md](ROADMAP.md) for a
planned `KividbRestore` CRD). Restoring means putting a snapshot back onto
a pod's `/data` volume before kividb starts reading it, which is
inherently a "the cluster is down for this" operation, so today it's a
manual procedure:

1. **Find the object key** you want to restore from a `KividbSnapshot`'s
   `status.objectKey`, then download and extract it locally:

   ```bash
   aws s3 cp s3://my-kividb-backups/prod/my-cluster/my-cluster-1-20260721T000004Z.tar.gz .
   # or: mc cp minio/my-kividb-backups/... .   (MinIO client)
   tar xzf my-cluster-1-20260721T000004Z.tar.gz   # produces dump.kdb (and appendonly.aof)
   ```

2. **Scale the StatefulSet to 0** to stop all kividb processes cleanly
   (avoids a running process overwriting the file you're about to copy
   in):

   ```bash
   kubectl scale statefulset my-cluster --replicas=0
   ```

3. **Copy the files onto the PVC** you want to restore into. The
   simplest way is a short-lived debug pod that mounts the same PVC:

   ```bash
   kubectl run restore-helper --rm -i --restart=Never \
     --image=busybox --overrides='{
       "spec": {"containers": [{"name":"restore-helper","image":"busybox","command":["sleep","3600"],
       "volumeMounts":[{"name":"data","mountPath":"/data"}]}],
       "volumes":[{"name":"data","persistentVolumeClaim":{"claimName":"data-my-cluster-0"}}]}}' &
   sleep 5
   kubectl cp ./dump.kdb restore-helper:/data/dump.kdb
   kubectl cp ./appendonly.aof restore-helper:/data/appendonly.aof   # if present
   kubectl delete pod restore-helper
   ```

   PVC names follow the StatefulSet's volume claim template naming:
   `data-<statefulset-name>-<ordinal>`, e.g. `data-my-cluster-0`.

4. **Scale back up**:

   ```bash
   kubectl scale statefulset my-cluster --replicas=<replicas+1>
   ```

   The operator's role-election logic will pick a master on next
   reconcile (within ~10s) and point the other pods' `REPLICAOF` at it —
   but note it will only find data on whichever pod(s) you actually
   restored into. If you're restoring the *whole* cluster from one
   snapshot (the common case — e.g. recovering from a bad write), restore
   the same `dump.kdb` onto **every** pod's PVC before scaling back up, so
   whichever one gets elected master has the data and the others don't
   diverge before their first `REPLICAOF` full resync overwrites their
   copy anyway.

5. Verify: `kubectl get kividbcluster my-cluster -o jsonpath='{.status}'`
   and connect with `redis-cli` to spot-check the restored keys.
