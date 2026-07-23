# Backups and restore

## How scheduled backups work

See [ARCHITECTURE.md](ARCHITECTURE.md#backups) for the full design. In
short: `spec.backup.enabled: true` makes the operator create a Kubernetes
`CronJob` named `<cluster>-backup` on `spec.backup.schedule`. Its pod runs
one command:

```
agent backup-trigger --url http://<cluster>-master.<namespace>.svc:8081/backup --timeout <spec.backup.timeoutSeconds>s
```

That's an HTTP POST to whichever pod currently holds the master role. The
master pod's own `agent` sidecar does the actual work: `BGSAVE`, wait for
`LASTSAVE` to advance, `tar.gz` `dump.kdb` (+ `appendonly.aof` if AOF is
enabled), stream it straight into your S3-compatible bucket, then delete
objects beyond `spec.backup.retention`.

Objects are stored at:

```
s3://<bucket>/<pathPrefix>/<cluster-name>/<pod-name>-<UTC timestamp>.tar.gz
```

e.g. `s3://my-kividb-backups/prod/my-cluster/my-cluster-1-20260721T000004Z.tar.gz`.

## Configuring backups

```yaml
spec:
  backup:
    enabled: true
    schedule: "0 * * * *"
    retention: 24
    s3:
      endpoint: "https://s3.us-east-1.amazonaws.com"
      bucket: "my-kividb-backups"
      region: "us-east-1"
      pathPrefix: "prod"
      credentialsSecretRef:
        name: my-cluster-s3-creds
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
kubectl create job my-cluster-backup-manual --from=cronjob/my-cluster-backup
kubectl logs job/my-cluster-backup-manual -f
```

## Checking backup status

```bash
kubectl get kividbcluster my-cluster -o jsonpath='{.status.backup}'
```

`lastRunTime`/`lastSuccessTime` mirror the CronJob's own
`status.lastScheduleTime`/`status.lastSuccessfulTime`. `lastError` is
populated from the most recent backup Job's failure condition and cleared
on the next success. **`lastObjectKey` is not currently populated** by the
controller (it would mean scraping Job pod logs, not yet implemented) —
find the exact S3 key a given run uploaded via
`kubectl logs job/<job-name>` (the `backup-trigger` client prints
`backup ok: object=<key> ...` on success) or by listing the bucket
directly.

A populated `lastError` alongside a stale `lastSuccessTime` means the
*most recent* run failed — check `kubectl get jobs -l kividb.io/cluster=my-cluster` and
`kubectl logs` on the most recent Job pod for the actual error (auth
failure, wrong bucket, network egress blocked, etc.).

## Restoring a backup

**There is no automated restore.** Restoring means putting a snapshot back
onto a pod's `/data` volume before kividb starts reading it, which is
inherently a "the cluster is down for this" operation, so the operator
deliberately doesn't automate it (an accidental automatic restore would be
much worse than a manual one). Procedure:

1. **Download and extract the snapshot** you want to restore, locally:

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
