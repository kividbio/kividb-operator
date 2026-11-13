# Changelog

All notable changes to kividb-operator are documented in this file. The
format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and versioning follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

**Apply the new CRDs before `helm upgrade`:** Helm does not upgrade them.
The `kividbclusters` CRD spec gains `failover.restartWaitSeconds`.
The `kividbconfigs`, `kividbaclconfigs`, and `kividbsnapshotconfigs` CRDs gain
a `status` subresource with `status.usedBy` tracking referencing clusters.
Until they are applied, the API server drops these fields.

**Read before upgrading:** the pods of every sharded cluster roll once,
replicas first and each master after a handover, for the new
`--cluster-announce-hostname` flag (#57). A shard with no replicas
restarts its master in place: its slots are unavailable while it does,
and with persistence off its data is lost. Scale such a shard to one
replica first, or upgrade in a maintenance window if persistence is off.

### Fixed

- **A restarted node of a sharded cluster showed as `?` in `CLUSTER
  SLOTS`** (#57), on itself and on its peers, for about six seconds: it
  started with no hostname, and the agent set one only later. A client
  that reloaded its slot map then could not reach those slots. A node now
  starts with `--cluster-announce-hostname`, its pod's DNS name, which
  every engine with `--cluster-announce-ip` (1.1.0 on) takes. The agent
  still sets the address of a Service a pod is exposed through.
- **A failover could promote a replica that held less than another, and
  did not wait for a force-deleted master coming back on its volume
  (#78).** In an AKS run, a force-deleted master lost 176 acknowledged
  writes, and the replica promoted was 640 bytes behind the other.
  - The replica is now picked after the old master's writes are held (or
    it is found gone), by each replica's offset read then with `ROLE`, not
    by the offsets read at the start of the pass, which may be from before
    the master stopped streaming. Replicas that hold as much go by name.
    The replicas are asked at once, for 1 s in all. One that does not
    answer may hold more than the one picked: unless that one caught up
    with the held master, no one is promoted on that pass
    (`FailoverBlocked` Warning, `WaitingForReplica` on the Ready
    condition) until it answers or is no longer Ready. A replica the
    operator cannot reach stays Ready (its readiness probe runs on its own
    node), so the Warning says how long the failover has been held and
    what ends it, and `kividb.io/allow-unread-failover: "true"` on the
    cluster lets it go ahead without that replica (`FailoverForced`
    Warning); the operator removes the annotation once the failover has
    gone through.
  - A master whose pod is gone, or replaced by a new pod (a force delete),
    and whose engine no longer answers, is now waited for until its new
    pod is Ready and has reported: it may come back on its volume holding
    writes its replicas do not. Then it is compared with them as a master
    that restarted in place is (#42), and kept if it holds as much. The
    wait lasts `failover.restartWaitSeconds` at most (new, 15 s by
    default, 0 for not at all), counted from when the master was found
    gone, whether its pod comes back or not, so a force-deleted master
    costs that much at most (and up to 5 s more: the wait is checked on
    each pass). A wait that ran out is not started again for that pod of
    the master. A master that restarts in place keeps
    `unhealthyThresholdSeconds`. A replacement pod that cannot be scheduled
    is not waited for, nor a master with no address or engine run
    recorded. The operator does not read Nodes, so that its RBAC stays as
    it is: a replacement pinned by its volume to a node that is not Ready
    shows as unschedulable, as the scheduler keeps new pods off such a
    node. The cap covers a volume that cannot attach yet: on AKS or EBS a
    replacement schedules on another node in the zone and sits in
    `ContainerCreating` (Multi-Attach), never unschedulable.
    `FailoverWait` events say when a wait starts and how it ended, and the
    Ready condition's reason is `WaitingForMaster` meanwhile.
- **`kubectl apply -k config/default` warned that `commonLabels` is
  deprecated** (#69). `config/default` sets the same two labels with
  `labels` instead, on each object's own labels only: the manager
  Deployment's pod template carries them in `config/manager`, and since
  #86 its selector does too. The rendered manifests are byte-for-byte
  identical, so nothing changes for an existing `config/default` install.
- **Status updates of a replicated cluster conflicted at steady state**
  (#52), each a `Reconciler error` with a growing backoff: 134 in a few
  hours on AKS. A pass that starts within milliseconds of the last one's
  status write reads the cluster from the cache before that write reaches
  it, and its own write then conflicts. The status is now written as a
  sharded cluster's is: after a change outside the status, the same
  status goes onto the latest version, also on the pass that fails over
  after it has recorded `status.fencedMaster`, and a pass that read a
  status a newer pass has since replaced leaves it alone and looks again
  in 5 s (2 s while a failover holds the old master's writes), logged at
  debug level only.
- **A refused bootstrap left the cluster's phase empty** (#51), and its
  `Bootstrapped` condition said `RestoreFailed` though no restore ran. A
  pass the bootstrap holds never reached the code that sets the phase, so
  it was empty through every bootstrap, and the GUI showed `Unknown`. The
  phase is now `Provisioning` while the bootstrap waits or restores, and
  `Error` once it has failed or was refused, with the reason `Refused` on
  the condition for a refusal. A restore started after the spec is fixed
  no longer keeps the refusal in `status.bootstrap.error`.
- **`make deploy` ignored `IMG`** (#63). It set the image `manager`, which
  no image in `config/manager` is named, and without `kustomize` on the
  PATH it set none: the manager ran `docker.io/kividb/kividb-operator:0.7.0`
  whatever `IMG` said. It now sets the image `manager.yaml` names, with the
  kustomize `make kustomize` installs into `bin/`.
- **`kubectl apply -k config/default` failed on a cluster without the
  `kividb-operator-system` namespace**, and so did `make deploy`: nothing
  created it. `make deploy` now creates it, and `docs/INSTALL.md` says to.
- **The GUI from `config/gui` could only run in `kividb-operator-system`**
  (#63). It mounts the agent token Secret from the operator's namespace,
  so with the operator anywhere else it had no token. `config/gui` is now
  a kustomization: `kubectl apply -k config/gui`, with its namespace set
  there or in an overlay. `kubectl apply -f config/gui/` works as before.

## [0.7.1] - 2026-11-12

A patch release of 0.7.0. The default engine is kividb **1.2.1**. Upgrade
now if you run engine 1.2.1, or will: since 1.2.1 the engine refuses a
write pause it cannot take at once, and operator 0.7.0 then switched over
without the pause, so a write the old master acknowledged could be lost
(#81). 0.7.1 also fences a force-deleted master's last writes before a
failover (#53), keeps a restore from mixing the snapshot with the earlier
cluster's append-only file (#58), fixes the images' CVEs (#76), adds an
OLM bundle for OperatorHub.io, and fixes three install paths.

**Read before upgrading: every cluster restarts, and a master with no
replica restarts in place.** The default agent image moves to 0.7.1, and
the default engine to 1.2.1, so every `KividbCluster` that leaves
`spec.agentImage` or `spec.image` unset gets a rolling restart when the
operator is upgraded. A master with replicas hands its role to an in-sync
replica first, as in every rollout. A master with none is deleted and
restarted on its volume, with no handover:

- **`replicas: 0` (one pod):** the cluster serves nothing until the pod is
  back.
- **A sharded cluster with `replicas: 0`:** each shard's master restarts
  in turn. Its slots are unavailable while it does, and if it is down for
  longer than the node timeout (5 s by default) with `requireFullCoverage`
  on (the default), the whole cluster refuses commands until it is back.
- **Data:** with the append-only file on (the default since 0.7.0) the
  master reloads everything it acknowledged. With `appendonly: "no"` it
  comes back with its shutdown snapshot only, and with nothing saved since
  its last snapshot if that save does not finish within
  `terminationGracePeriodSeconds`.

Before upgrading, give such clusters or shards at least one replica and
let it sync, or accept the restart: with the append-only file on, or with
the shutdown snapshot only. (Turning the append-only file on is a config
change, so it restarts such a master in place too.) To put the restart off, pin
both `spec.image: docker.io/kividb/kividb:v1.2.0` and
`spec.agentImage: docker.io/kividb/kividb-operator-agent:0.7.0` first:
nothing else in the pod template changes in 0.7.1. The 0.7.0 agent works
with the 0.7.1 operator, without the agent's fixes: it cuts every pause at
5 s, which aborts a switchover that needs longer (#81), and its restore
lacks the #58 fix. Unpin once the cluster has a replica or its
append-only file.

**Engine 1.2.1 has upgrade notes of its own.** The rollout restarts
replicas before their master, as 1.2.1 requires (a 1.2.0 replica cannot
full-sync from a 1.2.1 master that holds a key 1.2.0 cannot read). Read
the [engine's "Before you upgrade"](https://github.com/kividbio/kividb/releases/tag/v1.2.1)
first: among others, ACL categories are Redis's from the moment a pod
runs 1.2.1, `GEORADIUS` is refused on replicas, and there is no going
back to 1.2.0 once 1.2.1 has saved such a key. Do not change
`spec.shards` during the rollout: a slot move from a 1.2.0 node loses
the moved time series' compaction rules.

**The CRD change is additive, and upgrading from 0.7.0 is safe.** The
`kividbclusters` status gains `pods[].podIP` and `fencedMaster` (`name`,
`podIP`, `runID`), and new conditions (`Switchover`); nothing is removed
or renamed, and the other CRDs do not change. **Apply the new
`kividbclusters` CRD before `helm upgrade`:** Helm does not upgrade it.
Until it is applied, the API server drops the new fields and failovers do
not hold the old master's writes (`FailoverUnfenced` Warning). The CRDs
are attached to the GitHub Release. Coming from 0.6.x, read 0.7.0's
"Read before upgrading" first.

### Security

- **The images had golang.org/x/net v0.60.0 (CVE-2026-78661, high;
  CVE-2026-97045, medium) and an out-of-date Debian 12 base** (#76).
  golang.org/x/net is now v0.62.0, the other golang.org/x modules are
  current, the binaries are built with Go 1.26.11, and the images run on
  `gcr.io/distroless/static-debian13:nonroot` (still no shell, still UID
  65532). CI and the release workflow scan every image with Trivy and fail
  on a fixable HIGH or CRITICAL.

### Changed

- **Default engine `docker.io/kividb/kividb:v1.2.1`** (it was `v1.2.0`):
  in the controller, the CRD, the chart, the GUI, the samples and the docs.
  See "Read before upgrading" above.
- Default agent image **`docker.io/kividb/kividb-operator-agent:0.7.1`**.
  A cluster that leaves `spec.agentImage` unset rolls to it in the same
  rollout. The switchover and restore fixes below need it.
- The e2e suite tests kividb 1.2.1 by default.

### Added

- **`status.usedBy` on `KividbConfig`, `KividbAclConfig`, and `KividbSnapshotConfig`.**
  Tracks which `KividbCluster`s in the namespace reference each shared config object,
  displayed in `kubectl get -o wide` via a printer column.
- **An OLM bundle for OperatorHub.io.** `make bundle VERSION=… IMG=…` builds
  the bundle from `config/manifests` (operator-sdk v1.36.0, kustomize v5.4.3),
  and `make scorecard` runs the scorecard tests. The bundle validates for
  Kubernetes 1.27, 1.32 and 1.33 and installs through OLM 0.28.0 in
  AllNamespaces mode.

### Fixed

- **`make deploy` didn't pass `AGENT_IMG`** (#68). It updated the manager
  image but left the agent sidecar defaulted to the released 0.7.0 image.
  It now patches `--default-agent-image=$(AGENT_IMG)` into the manager
  Deployment.
- **The plain-YAML install and `kubectl apply -k config/default` made the
  manager Deployment with different selectors (#86).** Applying one over the
  other failed with `spec.selector: ... field is immutable`.
  `config/manager/manager.yaml` now selects on `app.kubernetes.io/managed-by`
  too, as `config/default` does; what `config/default` and the OLM bundle
  render is unchanged. A Deployment made before this from `config/manager`
  alone, by the plain-YAML install or `kubectl apply -k config/manager`, has
  the old selector: delete it once (`kubectl delete deploy -n
  kividb-operator-system kividb-operator-manager`) before applying the new
  manifests.
  (#88, by [SaiPisey2](https://github.com/SaiPisey2))
- **`docs/INSTALL.md`'s install without kustomize exited 1 (#67).**
  `kubectl apply -f config/rbac/` applied the RBAC and then failed on
  `config/rbac/kustomization.yaml`, and on a new cluster the namespace did
  not exist. The steps now create the namespace and name the RBAC files.
  (#85, by [SaiPisey2](https://github.com/SaiPisey2))
- **A switchover no longer hands over without holding writes when the pause
  could not take hold (#81).** Since kividb 1.2.1 (kividbio/kividb#629),
  `CLIENT PAUSE ... WRITE` waits up to 5 s for the writes already in progress
  and refuses, pausing nothing, if one has not finished (a big `FLUSHALL`, a
  `DEL` of a huge key, a long script). The switchover took that refusal, and
  a pause that got no answer within the agent's 5 s, for an engine before
  kividb 1.1: it promoted the target without waiting for it to catch up,
  and raised `SwitchoverWithoutPause` telling the user to upgrade an engine
  that was already new. A write the old master acknowledged then could be
  lost. Now such a switchover is called off (`SwitchoverAborted`) and tried
  again; a pause that got no answer is lifted, in case it landed. Only an
  engine that answers that it does not know the command is switched over
  without the pause: every kividb has it, so any other refusal (`LOADING`,
  `BUSY`) aborts too. The pause call is given 7 s, past the engine's wait
  (or `--agent-timeout`, if longer), and the agent is asked to wait 6 s:
  its exec request takes a `timeoutMs` (capped at 15 s), which the
  operator sends for the pause only. An agent before this release ignores
  it and cuts the pause at 5 s, which now aborts the switchover rather than
  dropping the pause. The engine counts the pause from the request, so it
  now lasts the call, the catch-up and a margin (17 s), and the old master
  is a replica before it runs out. Aborted switchovers back off: 30 s,
  doubling with each abort in a row up to 10 min, and 2 s between the tries
  for a master pod being deleted; a `Switchover` condition (`False`,
  `Aborted`) says why and when the next try is, until one goes through.
  The failover fence (#53) waits for a pause the same way once the old
  master has answered, and tries again after a refusal: every try is given
  the whole call, and new ones start for 3 s after the first, so the fence
  takes at most two pause calls and that window against a live master that
  cannot pause (17 s with the default `--agent-timeout`, more with a longer
  one). A pause that got no answer may have landed, so that
  master is recorded in `status.fencedMaster` and demoted after the
  promotion like a fenced one. The `Switchover` condition and the count of
  aborts go away once no switchover is wanted any more, or the master is
  another pod.
  (#82, by [lofoneh](https://github.com/lofoneh))
- **`kubectl apply -k config/default` installed no `KividbDbOps` CRD.**
  `config/crd/kustomization.yaml` did not list it, so the documented install
  could not run `KividbDbOps` operations. After applying it again, restart
  the manager (`kubectl rollout restart`): it looks for the CRD once, at
  start-up.
- **The full `KividbConfig` sample was rejected by its own CRD.** It set
  `tls: {enabled: false}`, and the CRD requires `certSecretRef` whenever `tls`
  is present. The sample now leaves `tls` out.
- **A restore over a kept volume could mix the snapshot with the earlier
  cluster's append-only file** (#58). The restore Job removes the earlier
  cluster's `dump.kdb.gen`, and without it engine 1.2.0 and 1.2.1 both
  replay `appendonly.aof` over `dump.kdb`. The Job removed the earlier
  cluster's `appendonly.aof` only after it had renamed the restored
  `dump.kdb` in, so a Job stopped between the two (killed, evicted, or a
  removal that failed) left the restored dump with the earlier cluster's
  log. A cluster started on that volume, once `spec.bootstrapFromSnapshot`
  was removed after a failed bootstrap, came up with the writes of both.
  Every file of the earlier cluster's engine now goes before a restored
  file takes its name: `appendonly.aof` and every `appendonly.aof.*`,
  every `dump.kdb.*` (its state and temp files), `dump.rdb`, and the
  `superseded.*` logs that engine 1.2.1 keeps aside. The engine never
  removes those: they held the volume's space, and every start named
  them with how to apply their writes. A Job stopped part-way now leaves
  what is left of the earlier cluster's files, or part of the snapshot,
  never both, and its retry ends with the snapshot's files only. The
  manual restore in `docs/BACKUP_RESTORE.md` removes the same files.
- **A force-deleted master's last writes were lost** (#53). Its pod object
  is gone at once and the operator fails over within the second, but its
  container runs on until its replicas have left it, acknowledging the
  writes that come in on connections clients already had to it: 176 lost in
  the AKS test on engine 1.2.1, 689 to 954 per force-delete on kind. Before
  promoting a replica, the operator now holds the old master's writes
  (`CLIENT PAUSE WRITE` through its agent, for the pod's termination grace
  period -- 10 s at least -- and 5 s more) at the address and engine run
  `status.pods[]` last recorded for it (`podIP`, new, and `runID`), waits
  for the replica to catch up with it (5 s at most, less once the old
  master stops answering), records it in `status.fencedMaster` (new), and
  once the replica is promoted makes the old master its replica and
  disconnects its clients, as a switchover does. Should that fail, or the
  operator restart, every pass holds its writes again and retries, every
  2 s, until it is done or the engine there is gone (another engine
  answers, or none does twice in a row); the first failure raises a
  `FencedMasterNotDemoted` Warning. An address that does not answer is
  asked again for about 1 s (one that refuses the connection, once); a
  pause that goes unanswered is sent again for up to 3 s. A failover that
  goes on without the fence, or before the replica caught up, raises a
  `FailoverUnfenced` Warning. A failed promotion no longer lifts the old
  master's pause: it may have gone through. The agent's preStop hook looks at a stopping master every 10 ms
  instead of 250 ms.
  `hack/e2e/10-force-delete-fence.sh` force-deletes the master under load
  and counts the writes lost (skipped in CI: `SKIP_FENCE=1`).
  (#77, by [lofoneh](https://github.com/lofoneh))

## [0.7.0] - 2026-11-04

Sharded clusters (`spec.shards`). The default engine is kividb **1.2.0**.

**Read before upgrading:** the agent's port (8081) is no longer on the
`<cluster>-master` and `<cluster>-replicas` Services, and the agent's API
needs a token. Anything that called an agent through those Services, other
than the operator's own backup Jobs, has to change. The pods roll once.
New backups go to `<pathPrefix>/<namespace>.<cluster>/` instead of
`<pathPrefix>/<cluster>/`: widen any IAM or bucket policy that limits the
backup credentials to the old folder, and adjust scripts and lifecycle
rules that match the old keys (see Fixed).

**Apply the new CRDs before `helm upgrade`:** Helm does not upgrade them,
and the `KividbCluster`, `KividbConfig`, `KividbDbOps` and `KividbSnapshot`
CRDs changed. Until they are applied, the API server drops the new fields
(`spec.shards` among them) and the checks that read them never fire. The
CRDs are attached to the GitHub Release.

**Read before upgrading: 0.7.0 targets kividb engine 1.2.0.**

- **A cluster without `spec.image` gets an engine upgrade** to the new
  default image, `docker.io/kividb/kividb:v1.2.0`, as a rolling restart:
  replicas first, masters last, each master only after an in-sync replica
  has taken over. Pin `spec.image` to stay on 1.1.x for now.
- **No going back to 1.1.x** once a 1.2 pod has saved a snapshot that
  holds a time series with compaction rules: 1.1.x cannot read it. Take
  a backup before the upgrade if you may need to downgrade.
- **During the rollout, 1.1.x replicas of a 1.2 master log errors.** A 1.2
  master sends a transaction, and a script that writes more than once, to
  its replicas between `MULTI` and `EXEC`; a 1.1.x replica still applies
  every write, but logs `Replicated command error (non-fatal): ERR unknown
  command 'MULTI'` (and `'EXEC'`) for each, until it is upgraded too.
- **The append-only file is on by default** (`appendonly yes`,
  `appendfsync everysec`), for every cluster whose `KividbConfig` does not
  set `appendonly` itself. It comes with the rolling restart above. Writes
  go through the file, so expect fewer writes a second at high
  concurrency, and size the volumes for the log too. `appendonly: "no"` in
  a `KividbConfig` keeps the old behaviour, with a `Persistence` condition
  and a Warning Event that say what it risks: without it, a killed engine
  comes back with only its last shutdown or full-sync snapshot, often
  nothing (see Fixed).
- **A `KividbConfig` directive the engine refuses stops the server.** 1.2
  accepts Redis's own settings with a warning and applies some it used to
  ignore, but a name that is neither KiviDB's nor Redis's, or a bad value,
  is still fatal. A rolling change stalls safely after the first pod; a new
  cluster or a cold restart has every pod down. See
  [CONFIGURATION.md](docs/CONFIGURATION.md#references-to-other-crds).
- **A `KividbSnapshotConfig` whose `spec.schedule` has a `TZ=` or
  `CRON_TZ=` prefix is now refused** as `InvalidSpec` (#47): a CronJob
  does not accept one. Every cluster that references it holds back its
  apply steps until it is fixed, though failover still runs. Remove the
  prefix before upgrading.
- **Reshard only under light write load.** Engine 1.2.0 can lose writes
  while slots move between shards under heavy writes
  (kividbio/kividb#331). That covers changing `spec.shards` (scaling in
  or out, which uses `CLUSTER MIGRATION IMPORT`) and `redis-cli --cluster
  reshard`. It was seen only under heavy CPU contention. Until engine
  1.2.1, change `spec.shards` while writes are light, or pause writes.
  Engine 1.2.1 fixes it: set `spec.image` to it once it is released.

### Security

- **The agent's API needs a token.** In 0.6.0 any client that could reach
  port 8081 of a kividb pod could promote it, repoint its replication, or
  run any command through `/exec`; and the port was on the master and
  replica Services, which can be LoadBalancers. Now every endpoint but
  `/healthz`, `/readyz` and `/metrics` requires `Authorization: Bearer
  <token>`, and an agent without a token refuses them. The operator keeps
  the token in a Secret in its namespace (created with a random value at
  its first start; `--agent-token-secret`, the chart's
  `<release>-agent-token`) and copies it into each cluster's auth Secret.
  The operator, the backup Jobs and the GUI send it; the GUI reads it from
  the Secret mounted as a file. A new token rolls the pods, as a new
  password does.
- **The agent port is on the headless Service only**, not on the master
  and replica Services.

### Added

- **`spec.shards`**: sharded clusters in kividb's cluster mode, one
  StatefulSet per shard (`<cluster>-<shard>`), a PodDisruptionBudget per
  shard, and ClusterIP Services only: one per pod, one per shard,
  `<cluster>-replicas`, `<cluster>` for the whole cluster, and a headless
  Service per shard. `spec.cluster` sets the node timeout, full coverage,
  `min-replicas-to-write` and the bus secret (generated when unset). The
  operator owns every cluster-mode directive. A cluster is sharded or not
  from its creation; up to 24 nodes for now.
- **Sharded clusters form and look after themselves.** The operator
  introduces the nodes (`CLUSTER MEET`), assigns the slots evenly across the
  shards once every pod is up, puts each shard's replicas behind its
  master, and has every node announce its pod's DNS name. Failover is the
  engine's: the operator reports it and moves the role labels, and never
  promotes in cluster mode. It re-introduces nodes after IP changes (a
  cold restart), and forgets the old ID of a pod that rejoined with a new
  one, never a node that owns slots. The agent gains `/cluster/status`,
  `/meet`, `/addslots`, `/replicate`, `/forget` and `/announce`, all behind
  the agent token.
- **Rolling restarts of sharded clusters**, for a template change or a
  `KividbDbOps` restart: one pod at a time across the cluster, replicas
  first, and each shard's master only after its most caught-up replica
  has taken over with `CLUSTER FAILOVER`. The agent has a
  `/cluster/failover` endpoint, and reports whether a replica's link to
  its master is up; `status.pods[].synced` is set for sharded clusters.
- **Changing a sharded cluster's shard count.** Edit `spec.shards` and
  the operator adds shards, or drains and removes them, moving only the
  slots it has to: one move at a time, each an atomic `CLUSTER MIGRATION
  IMPORT` on the destination master (no `ASK` for clients). Phase
  `Rebalancing`, with the plan, the move under way and the last error in
  `status.cluster.rebalance`. `status.cluster.shards` is the count the
  slots are spread over. Removed shards' PVCs are deleted once their
  slots have all moved out. The agent has
  `/cluster/migration/import` and `/cluster/migration/status`.
- **Backups of sharded clusters.** A run backs up every shard at once,
  each from a pod with the configured role, under
  `<namespace>.<cluster>/<run>/shard-<i>-<pod>.tar.gz`. A `manifest.json`
  names each shard's object, source, master node ID and slots, and is
  written only when every shard succeeded. Retention counts complete runs
  and prunes whole runs. `KividbSnapshot.status.shards[]` lists the
  shards, and `objectKey` is the manifest. The agent has
  `/backup/manifest`, and `/backup` takes a run and shard. Restoring a
  sharded cluster is not supported yet.
- **Access to a sharded cluster from outside Kubernetes.** Expose each pod
  with a Service of your own (a LoadBalancer, a NodePort) labelled
  `kividb.io/announce: "true"`, and the pod's node announces that
  Service's address (its load-balancer address, or the
  `kividb.io/announce-address` annotation) instead of its DNS name, so
  cluster clients outside follow `MOVED` to an address they can reach.
  The operator only reads these Services. The `ExternallyAccessible`
  condition says whether every pod is exposed (`PartiallyExposed` when
  only some are) and what is wrong with a Service that cannot be used.
- **The console shows sharded clusters.**
  - **Shards:** one card per shard, with its slots, master and replicas, and each replica's lag against its own shard's master.
  - **Slot map:** the slots coloured by shard; the ones moving are striped.
  - **Rebalance:** progress while the shard count changes.
  - **Tiles:** slots and `cluster_state`.
  - **Actions:** **+/− Shard**. **Promote** runs `CLUSTER FAILOVER` on the replica. **Take snapshot** starts a backup run of every shard, as a Job from the backup CronJob, so the GUI's ClusterRole gains `create` on `jobs`.
- **`spec.cluster.replicaValidityFactor`**: `cluster-replica-validity-factor`,
  which engine 1.2 enforces: a replica whose link to its master has been
  down for longer than `nodeTimeoutMs × replicaValidityFactor` does not
  stand for election. Unset, the operator writes nothing and the engine's
  default, 10, applies (with the default 5 s node timeout, a replica cut
  off for more than about a minute does not take over); `0` lets every
  replica stand. Needs engine 1.2: older engines do not start with the
  directive. A `KividbConfig` may not set it.
- **Handovers that do not happen are reported.** Engine 1.2 pauses a
  shard master's writes for up to 10 s during `CLUSTER FAILOVER`, and the
  replica gives the failover up after 5 s (`Manual failover abandoned`).
  The `ShardSwitchover` Event now marks only the first request of a
  handover; a `Warning` `ShardSwitchoverAbandoned` follows when the master
  has not moved 15 s after a request. The operator asks at most 5 times
  in a row (then again after 10 minutes), and the `ShardSwitchover`
  condition shows the shard, the pods and the attempt while a handover is
  pending (`InProgress`, `Retrying`, `Stalled`).
- **A `KividbDbOps` restart no longer waits forever for a master to hand
  over.** After 5 minutes it fails, says where to look, and withdraws its
  step-down request. `status.restart.handoverStartTime` records when it
  started waiting.

### Changed

- **Default engine `docker.io/kividb/kividb:v1.2.0`** (it was `v1.1.3`):
  in the controller, the CRD, the chart, the GUI, the samples and the docs.
  The GUI's create form still adds `-tls`, `-lua` or `-full` for a variant.
  See "Read before upgrading" above.
- Default agent image **`docker.io/kividb/kividb-operator-agent:0.7.0`**.
  A cluster that leaves `spec.agentImage` unset rolls to it in the same
  rollout. Several fixes below need it.
- The e2e suite tests kividb 1.2.0 by default.
- CI runs the core e2e suites on a kind cluster for every pull request
  (#20, by [nightmare-tech](https://github.com/nightmare-tech)).
- The docs say what engine 1.2 adds to a pod's stop and memory: a master
  with replicas waits up to `shutdown-timeout` (10 s) for them before its
  final save, inside `terminationGracePeriodSeconds`; and the 64 MB
  `repl-backlog-size` counts in `maxmemory`, so a `maxmemory` of tens of
  megabytes needs `repl-backlog-size: "1mb"`.
- **The rendered `kividb.conf` is a Secret** (`<cluster>-config`), not a
  ConfigMap, for every cluster: a sharded cluster's carries the bus secret
  and `masterauth`. The old ConfigMap is removed once no pod mounts it;
  the pods move to the Secret as they roll.
- The operator's ClusterRole adds PodDisruptionBudgets.
- The backup Job finds the pod to back up by asking every pod's agent for
  its role, through the headless Service's per-pod names, instead of going
  through the master or replica Service.
- A `KividbConfig` that sets `cluster-enabled` to anything but `no` is
  refused (the cluster's `Ready` condition says why). Cluster mode would
  fight the operator's own failover; sharded clusters come as
  `spec.shards`.
- In a sharded cluster, a `KividbConfig` may set the slot migration
  settings, `cluster-slot-migration-handoff-max-lag-bytes` and
  `cluster-slot-migration-write-pause-timeout` (applied at runtime since
  engine kividbio/kividb#111). Every other `cluster-*` directive is still
  the operator's.
- **The console's Promote of a shard replica reports a request**, not a
  promotion: `CLUSTER FAILOVER` answering `OK` only means the failover
  started. The endpoint answers `202 {"requested": pod}` and the page
  refreshes a few seconds later to show whether the role moved.
- The docs say what engine 1.2 fixes: a failed import's keys are deleted
  at restart (#108), `cluster-announce-client-port` exists but is not used
  yet (#110), heartbeats keep one connection per peer though the 24-node
  cap stays (#112), replicas resync in part after a restart since 1.0.5
  and after a failover since 1.2 (#109), elections favour the most
  caught-up replica (#107), and certificates can be reloaded with `CONFIG
  SET`, not used yet. `KIVIDB_ENGINE_ISSUES.md` is marked frozen at 1.0.5,
  with a table of the later engine issues and the version that fixes each.
- The e2e harness documents `KIVIDB_IMAGE_BASE=ghcr.io/kividbio/kividb`
  for engine release candidates, which are on GHCR only.

### Fixed

- **A cluster re-created at once with a deleted one's name and
  `bootstrapFromSnapshot` came up on the deleted cluster's data, saying
  `Completed` / skipped** (#43, new in this release). Right after saving
  `BootstrapWaiting`, the pass read the cluster back from a cache that
  had not seen it yet and updated the earlier StatefulSet; that moved
  its owner reference to the new cluster, so it was never
  garbage-collected. While the bootstrap runs, the cluster now never
  updates a StatefulSet it does not control by UID, and only one of its
  own with pods (above 0 replicas), and no restore Job of its own,
  counts as running. A `<cluster>-bootstrap` Job from the earlier
  cluster is waited for too, not taken for this cluster's restore, and a
  restore onto a pod-0 volume older than the cluster says so in
  `status.bootstrap` and with a `BootstrapOverExistingVolume` Warning.
  (#46, by [SaiPisey2](https://github.com/SaiPisey2))
  A pod-0 PVC that is being deleted is waited for too: the restore Job
  starts once it is gone, on a new one. The wait shows in
  `status.bootstrap.message` ("waiting for PVC data-<cluster>-0 to finish
  deleting") and in a Normal `BootstrapWaitingForVolume` Event, not only in
  the operator's log (#55). A new condition, `Bootstrapped`, says where a
  bootstrap is: `False` with reason `WaitingForEarlierCluster`,
  `WaitingForVolumeDeletion`, `Restoring`, `RestoreRetrying` or
  `RestoreFailed`, and `True` once restored (or skipped).
- **`status.bootstrap` said `Failed` while the restore Job still had a
  retry left** (#54, also in 0.6.x). A failed pod was taken for a failed
  Job, and the retry then went on to complete. The bootstrap now fails
  only once the Job's `JobFailed` condition is true (its `backoffLimit`
  used up or its deadline passed), with a `BootstrapFailed` Warning Event.
  Until then it stays `InProgress`, and the message says "restore Job
  attempt N failed, retrying" with the failed pod's reason, with a
  `BootstrapRetrying` Warning Event per attempt. The restore container's
  last log lines are its termination message, so the reason includes them.
- **A bootstrap onto a kept PVC that still held the engine's `dump.kdb`
  failed and destroyed that file** (#44, also in 0.6.x). The restore Job
  runs as a different UID from the engine: it overwrote the file, then
  could not chmod it. It now writes each file to a temp file next to it
  and renames it in only once the whole archive has been read, so a
  failed restore leaves the old files whole. The volume needs room for
  the old and the restored files at once; a full one fails with an error
  that says so. A `dump.kdb` or `appendonly.aof` that the snapshot does
  not have is removed, so a log left from the earlier cluster is no
  longer replayed on top of the restored data.
  (#45, by [SaiPisey2](https://github.com/SaiPisey2))
  The engine's other files from the earlier cluster are removed too, before
  the restored files go in: `dump.kdb.gen`, its save number, made pod-0
  refuse to start or skip the snapshot's log and lose its writes, and
  `appendonly.aof.prev` (with `.prev.len` and `.rewrite.tmp`), from a
  checkpoint that did not finish, was replayed on top of the restored dump.
  `dump.kdb.repl` and `dump.kdb.clean` go as well. A restore that fails
  before then still leaves them all in place, with the old dump and log.
- **A master that restarted with the append-only file on was failed over
  to a replica behind it** (new in this release). A killed engine starts a
  new stream, which was taken as lost writes. On engine 1.2.0 or later
  with `aof-ack-after-write yes` (its default), which acknowledges a write
  only once it is in the file, a master holding at least as many keys as
  the replica now stays the master, and the replicas resync from it. Key
  counts miss a lost overwrite or delete; the replicas resync without it.
  Any other master is still failed over, as is one whose agent does not
  report the engine's version and that setting (the agent's `/status` now
  carries both): engine 1.1.x acknowledges a write before it is in the
  file, so a killed master can lack writes its replica holds.
  (#42, by [SaiPisey2](https://github.com/SaiPisey2))
- **A `KividbDbOps` restart deleted a master in place when its replica was
  not in sync at that moment, and did not wait for the other pods or ops**
  (also in 0.6.x), so two restarts of one cluster could take two pods
  down. A master with a replica now always hands over first, and each pod
  is restarted only while every other pod is Ready, in sync and not being
  deleted, as in a rolling update (a replica that is the only unhealthy
  pod goes first; a wait of more than 5 minutes for another pod, or 60 for
  a restarted one, fails the op, naming the pod). The ops of one cluster
  run one at a time, oldest first; the others stay `Pending` with a
  `Queued` condition.
  (#40, by [SaiPisey2](https://github.com/SaiPisey2))
- **`spec.bootstrapFromSnapshot` added to a running cluster scaled it to 0
  and restored the snapshot over pod-0's data** (also in 0.6.x). The CRD
  now refuses the field after creation, and a cluster whose StatefulSet
  already exists skips the bootstrap with a `BootstrapSkipped` Event. A
  StatefulSet and its pods left from an earlier cluster with the same name
  are waited for (`BootstrapWaiting`), not skipped and not touched;
  removing the field cancels the wait. Helm does not upgrade CRDs: until
  the new `kividbclusters` CRD is applied, the API server still accepts the
  field, and only the skip protects the cluster.
  (#37, by [SaiPisey2](https://github.com/SaiPisey2))
- **`FailoverBlocked` held for one pass, then an empty replica was
  promoted** (also in 0.6.x). The blocked pass emptied `status.pods`, so
  the next one no longer knew the master had held keys, and the old
  master later resynced from the empty replica. A failed pass now keeps
  the last `status.pods` (with no pod `Ready`), and a newly promoted
  master keeps its key count.
  (#35, by [SaiPisey2](https://github.com/SaiPisey2))
- **A failed reconcile step turned off failover, with the cluster still
  `Ready`** (also in 0.6.x). A refused object or a missing referenced
  KividbConfig, KividbAclConfig, KividbSnapshotConfig or Secret now
  holds back the writes after it, not failover, role labels or status:
  `Ready` is `False` (`ReconcileError` or `InvalidSpec`), in sharded
  clusters too, with a Warning Event of that reason (#28). The Event
  comes once per change of the error, not every pass, and not for a
  conflict, which the next pass retries. Port clashes (`spec.port`,
  `tls.port`, the agent's 8081, the exporter's 9121) and backup
  schedules a CronJob would refuse (not valid cron, or with a `TZ=` or
  `CRON_TZ=` prefix) are refused before anything is written. The
  referenced kinds are watched, so a fix takes effect at once.
  (#36 and #47, by [SaiPisey2](https://github.com/SaiPisey2))
- **Removing a shard could stay in `Rebalancing` for good** (new in this
  release). On a status conflict, a pass that had read the cluster a
  version behind wrote its status over the latest one, and could bring
  back a change that was already finished. The removed shard's
  StatefulSet was gone by then, so later passes waited for pods that never
  came back. Now a status that changed since the pass read it is not
  written over, a removed shard whose StatefulSet is gone is not waited
  for, and a shard made again with no slots, when `spec.shards` was
  raised back meanwhile, gets its master and its share.
  (#38, by [SaiPisey2](https://github.com/SaiPisey2))
- **Backup retention deleted other clusters' backups** (also in 0.6.x). A
  cluster pruned everything under `<pathPrefix>/<cluster>/`, which also
  held the backups of a same-named cluster in another namespace, or of a
  cluster nested under it by `pathPrefix`; two such clusters could even
  write the same key. Backups now go to
  `<pathPrefix>/<namespace>.<cluster>/` (the agent gets `POD_NAMESPACE`),
  and retention deletes only this cluster's own archives or runs there.
  Older backups still restore but are no longer pruned (see
  [BACKUP_RESTORE.md](docs/BACKUP_RESTORE.md) before deleting them), and a
  0.6.x agent, until upgraded, still prunes all of its
  `<pathPrefix>/<cluster>/`, which holds new keys only of a cluster whose
  `pathPrefix` is that folder or lies under it.
  (#41, by [SaiPisey2](https://github.com/SaiPisey2))
- **A new agent token stalled the rollout of every cluster with 2+ pods**
  (new in this release). The agent read `AGENT_TOKEN` once, at start, so
  the running pods answered the operator's new token with 401 and the
  rollout that was to give it to them waited for good. The agent now reads
  `agent-token` from the auth Secret it already mounts, on each request,
  falling back to `AGENT_TOKEN`; the pods take a new token once the kubelet
  refreshes the mount (a minute or two), so a rollout may wait that long.
  (#39, by [SaiPisey2](https://github.com/SaiPisey2))

Found by a third end-to-end test on AKS (operator master 853cd01 on an
engine 1.2.0 candidate):

- **A force-killed shard master lost its whole shard's data** (new in
  this release: sharded clusters). The engine never snapshots on a
  schedule, and the operator wrote no `appendonly` (for any cluster), so
  there was no persistence at all. The master, deleted with
  `--grace-period=0 --force`, was back in about a second -- inside the 5 s
  node timeout, so no failover -- with its node ID, its slots and 0 keys.
  Its replica refused to full-sync from a master with no keys, but asked
  again every 10 s, and once the master had taken a few writes it went
  through: "Dropped the 25939 local keys before loading 987". The cluster
  stayed `Ready`, with no Event; a second kill lost 53,271 more. With
  `appendonly yes` the same kill lost nothing. Now:
  - **The append-only file is on by default**, for sharded and replicated
    clusters alike: the operator writes `appendonly yes` and `appendfsync
    everysec` unless the `KividbConfig` sets `appendonly` (or `aof`)
    itself. `appendonly: "no"` opts out; the new `Persistence` condition
    is then `False` (`AppendOnlyDisabled`) and a `Warning` Event
    `PersistenceDisabled` is recorded when it turns so. See
    [CONFIGURATION.md](docs/CONFIGURATION.md#persistence-the-append-only-file)
    for the cost (write throughput, disk) and the trade-off.
  - **A shard master that restarted with less than its replica holds is
    failed away from.** When a shard master's engine started less than 2
    minutes ago and a replica of its shard holds more than twice its keys,
    the operator holds the master's writes (`CLIENT PAUSE WRITE`, which
    also keeps an empty master empty, so the replica's full-sync guard
    keeps refusing it) and has the replica take over with `CLUSTER
    FAILOVER FORCE`, then `TAKEOVER` (at once with a single shard). It
    asks a restarted master's agent before the pod is Ready. Events
    `MasterRestartedEmpty`, `MasterRestartedEmptyResolved` and, when the
    replica had resynced first or no replica held more, `ShardDataLost`;
    condition `MasterRestartedEmpty` (`FailingOver`, `Stalled`,
    `FailedOver`, `DataLost`) for 10 minutes. This is a safety net, not a
    fix: a client that kept the pod's address (a container restarted in
    place) writes to it at once, and the replica's first resync, about a
    second after the restart, then goes through before any controller can
    act. In a kind test without persistence, a forced delete lost 54 and
    65 writes instead of the shard (takeover 0.7-1.0 s after the engine
    started); an in-place kill still lost the shard (the replica resynced
    0.16-0.43 s after the engine started), reported as `ShardDataLost`. The agent's `/cluster/status` reports the engine's `run_id`,
    uptime, key count, `aof_enabled` and `loading`, and
    `/cluster/failover` takes `{"mode": "force"|"takeover"}`; a sharded
    cluster's `status.pods[]` record `keys`, `runID` and `incarnation`.
    Needs the 0.7.0 agent image.
- **Every planned handover of a shard master was also reported as a
  `Warning` `ShardFailover`** (new in this release). The move was taken as
  planned only if the old master's pod still had its step-down annotation
  or deletion timestamp when the move was seen, and a deleted pod was by
  then gone or back without them. The operator now remembers the
  handovers it asked for (2 minutes), and the console's Promote notes the
  replica it promoted on the cluster (`kividb.io/shard-promote`) before
  asking for it: both are `Normal` `ShardSwitchover`.
- **A sharded password change recorded no `PasswordChanged` Event**, and
  other Events went missing after a rollout. client-go drops an object's
  Events past 25, then lets one through every 5 minutes; one rollout of a
  sharded cluster used that up. The operator's limit is now 100, then one
  every 5 seconds.
- **Removing a shard made its StatefulSet again** (new in this release).
  On 4 → 3 the operator deleted the removed shard's StatefulSet and
  volumes, and a pass that still read the change as under way (its status
  a moment behind) created the StatefulSet again: two new pods on two new
  disks, removed again 30 s later. A removed shard's StatefulSet (and
  disruption budget) is now only updated while it exists, never created,
  and it is deleted in the foreground, so it stays until its pods are
  gone.
- **A replicated cluster bootstrapped from a sharded cluster's backup**
  ran its restore Job on the run's `manifest.json` ("gzip: invalid
  header"), failed, and kept saying "restore Job running". It is refused
  before any Job runs (`BootstrapRefused`), and a failed restore Job's
  message says so.

Found by a second end-to-end test on AKS (operator master on engine
v1.2.0-rc2):

- **A false `MasterRestartedBehind` failover right after a planned
  switchover lost 314 acknowledged writes** (new in this release). The
  check compared the new master's offset with one recorded for it a moment
  earlier, readings taken at different times, and failed over from a
  master that had never restarted; the master it failed away from kept
  taking writes until its resync from the replica promoted in its place
  discarded them. Now the check needs evidence that the master's engine
  restarted (a new engine `run_id`, or, from an older agent, a new pod or
  a container restart), compares offsets only within the stream the pod
  reported writing as the master (`status.pods[].replID`, cleared when the
  role moves), and skips replicas already resynced from the restarted
  master. When it does fail over, the restarted master's writes are held
  (`CLIENT PAUSE WRITE`) until it follows the new master, as in a
  switchover. The agent reports the engine's `run_id` and, on a master,
  its `master_replid`; `status.pods[]` gains `replID`, `runID` and
  `incarnation`. Helm does not upgrade CRDs: apply the new
  `kividbclusters` CRD, or these fields are dropped and the check never
  fires.
- **A rollout waiting on a replica that was not in sync gave no sign**
  (also in 0.6.0 for replicated clusters). The rollout rightly stops, in
  a sharded cluster cluster-wide, but the cluster went on reporting
  `Running` and `Ready` with no Event and no log line; a frozen replica
  was found only because nothing moved. A new condition,
  `RolloutProgressing`, says what the rollout is doing: `ReplacingPod`,
  or `WaitingForReplicaSync` / `WaitingForPodReady` naming each pod it
  waits for and why (link up or down, bytes behind the master, last I/O
  from it). After 2 minutes on the same pods it turns `False`, with a
  `RolloutWaiting` Warning Event and a log line, repeated every 10
  minutes while the wait lasts. The rollout still waits as before. The
  agent reports `master_last_io_seconds_ago` in its cluster status too.
- **GUI: a cluster's operations list was empty for a `KividbDbOps`
  created with kubectl.** It was filtered by a `kividb.io/cluster` label
  that only the GUI's own DbOps carry; it now goes by `spec.clusterRef`.
- **GUI: promoting a shard's replica left no Event.** The GUI asks the
  engine for `CLUSTER FAILOVER` itself, so the operator never saw it; the
  GUI now records a `ShardSwitchover` Event on the cluster, as the
  operator does for its own handovers (and as a replicated cluster's
  promote gets a `Switchover` Event). The GUI's ClusterRole gains
  `create` on `events`.

Found by an end-to-end test on AKS (replicated: 1 master + 2 replicas
under 800 writes/s; sharded: 3 × (1 + 2)), which checked every
acknowledged write afterwards.

- **A planned switchover lost the writes acknowledged during it** (also in
  0.6.0 and earlier). A rollout, a `KividbDbOps` restart, a password or
  config change, or the GUI's promote moved the master role by relabelling
  the old master, promoting the replica and repointing the old master,
  with nothing to stop the old master acknowledging writes in between, nor
  any wait for the replica to have the writes it had already acknowledged;
  and the old master was deleted on the next pass. Each switchover in the
  test lost the writes of that second: 19, 25, 23 and 64. Now the old
  master's writes are held (`CLIENT PAUSE ... WRITE`) until the replica has
  applied everything up to the master's offset, at most 5 s, and only then
  is the replica promoted, the old master made its replica, the label
  moved and the pause lifted; if the replica does not catch up, the pause
  is lifted and the old master stays (`SwitchoverAborted`). The label goes
  onto the new master before it comes off the old one: the master Service
  was without an endpoint for about 1.5 s (connection refused), it no
  longer is. The old master's clients are disconnected, so they reconnect
  to the new master rather than get `READONLY`. The GUI's promote now asks
  the operator for this (the `kividb.io/switchover-to` annotation on the
  cluster) instead of promoting the pod itself.
- **A replica whose link to the master was down counted as in sync, and
  was promoted** (also in 0.6.0). In a replicated cluster "in sync" looked
  only at the offset, allowing 8 MiB of lag -- minutes of writes at a
  modest rate -- so a restarted replica that could not authenticate to its
  master, its offset frozen, was taken as in sync, and a switchover
  promoted it 541 KB behind: 13,455 and 13,919 acknowledged writes lost.
  In sync now needs `master_link_status:up`, something heard from the
  master in the last 20 s, and at most 1 MiB of lag (the agent reports the
  link and `master_last_io_seconds_ago`); a switchover target must also be
  following the master with its link up, and catch up exactly.
- **Removing the password broke replication during the rollout** (also in
  0.6.0): the restarted pods had no password while the old master still
  required one, so their replication links stayed down -- what made the
  13,455 writes above. And **a new password was refused until the master
  was restarted** (11 s of authentication errors in the test): the master
  kept the old one until it was replaced. Now the pods still on the
  previous password are given the new credential live (`ACL SETUSER
  default #<hash>`, or `nopass` for a removal) before any pod restarts,
  pods started during the rollout accept both, and the previous password is
  dropped, by an ACL reload on every pod, once every pod runs with the new
  one. See [CONFIGURATION.md](docs/CONFIGURATION.md#changing-the-default-users-password).
- **Deleting, draining or evicting the master pod lost writes and took the
  cluster down for about 14 s** (also in 0.6.0). Nothing handed the role
  over: the pod came back as the master from its shutdown snapshot, and the
  replicas, which held every acknowledged write, were resynced from it (92
  lost). Now both containers have a `preStop` hook (the agent's `/prestop`)
  that holds a master with replicas attached -- for at most a third of the
  grace period, 5 to 30 s -- while the operator, seeing the pod's deletion,
  does the switchover above; the engine then stops as a replica. The same
  holds for a shard master, which hands over with `CLUSTER FAILOVER`. A
  replicated cluster now has a PodDisruptionBudget (`maxUnavailable: 1`),
  as each shard has.
- **A master that restarted behind its replicas had them resync from its
  older data** (also in 0.6.0). When the master comes back with a lower
  replication offset than the operator last recorded for it while a
  replica that was in sync holds more, that replica is promoted instead,
  and the restarted pod becomes its replica (`MasterRestartedBehind`). The
  status keeps a pod's last offset and key count while it cannot be asked.
  An in-place container restart, which keeps the pod's address, is still
  resynced by the engine before the operator can act.
- **A hung engine was never restarted** (also in 0.6.0). The kividb
  container's liveness probe was a TCP check, which a deadlocked engine
  passes: it accepts connections and answers nothing. It is now the
  agent's `/livez`, which passes on any reply to `PING` (`LOADING` and
  `BUSY` too) and fails after about 90 s of none, after the failover of a
  hung master; a startup probe gives an engine up to 30 minutes to load
  its data first. Needs the 0.7.0 agent image.
- **The operator forgot nodes of sharded pods that were only unresponsive.**
  A pod whose agent did not answer lost its node ID from the status at
  once, and its node, marked failed by the others and owning no slots, was
  then taken for "a node that is none of the pods" and forgotten
  (`CLUSTER FORGET`) on every node; the pod could only rejoin once the
  forget ban expired. The status now keeps such a pod's node ID, a node is
  also recognised as a pod's by its address and announced name, and a node
  that is no pod's is forgotten only after 2 minutes. A node of an existing
  pod is forgotten only when that pod answers with a new node ID.
- **`/readyz`, `/status` and `/metrics` failed while a password was being
  removed** on engine 1.2 (also in 0.6.0). The agent logged in with
  `AUTH <password>`, which engine 1.2 refuses while the `default` user has
  no password (kividbio/kividb#135), as when the password is gone from the
  server and a pod still has the old Secret. The agent now always sends
  `AUTH default <password>`, which every engine takes; it sets
  `masteruser default` with `masterauth`, so replicas log in to their
  master the same way; and the exporter gets `REDIS_USER=default`. The
  new exporter variable rolls the pods of clusters with a password.
- The chart's ServiceMonitor scraped each pod twice, through the headless
  Service and through its master or replica Service. It selects the
  headless Service only (`kividb.io/service: headless`).

## [0.6.1] - 2026-11-01

A patch release of 0.6.0. The default engine is kividb **1.1.3**, a
security release of kividb 1.1.

### Security

- **Default engine `docker.io/kividb/kividb:v1.1.3`** (it was `v1.1.2`):
  in the controller, the CRD, the chart, the GUI, the samples and the docs.
  **A `KividbCluster` that leaves `spec.image` unset moves to 1.1.3 when the
  operator is upgraded**, the way every rollout goes: the replicas are
  restarted first, then the operator switches over to an up-to-date replica,
  and the old master is restarted last. Set `spec.image` to stay on a
  version.

### Changed

- Default agent image **`docker.io/kividb/kividb-operator-agent:0.6.1`**.
  A cluster that leaves `spec.agentImage` unset rolls to it in the same
  rollout.
- The e2e suite tests kividb 1.1.3 by default.

## [0.6.0] - 2026-10-24

A new management GUI. The default engine stays kividb **1.1.2**.

**Read before upgrading:** with GUI authentication on, the GUI now has its
own sign-in page and session cookie in place of the browser's Basic-auth
box. Scripts that call the API with Basic auth (`curl -u`) keep working.

### Added

- **A sign-in page** (`/login`). Signing in sets a session cookie
  (HttpOnly, SameSite=Lax, Secure over HTTPS, 12 hours), signed with a
  random key made when the GUI starts: nothing in it is derived from the
  password, and a restart (which changing the password needs) ends every
  session. Pages redirect to it when signed out,
  and come back to where you were; the API answers 401 without the header
  that makes browsers show their own login box. Ten failed attempts from
  one address lock it out for five minutes. **Sign out** is in the top
  bar. Basic auth still works for scripts.
- **Dashboard overview.** Summary tiles (clusters; healthy, needing
  attention, in error; memory, commands per second and clients across all
  pods), a search box and a health filter, and for each cluster its engine
  version and build, a pod bar, and sparklines of the last hour's commands
  per second and memory. The figures come from the GUI's own scrapes, so
  the dashboard is still one `List` of `KividbCluster`s.
- **Topology** on a cluster's page: the master and its replicas, each with
  its readiness, memory, keys, replication offset and node, and how far each
  replica is behind the master.
- **Charts** with axes, a hover readout of every pod, a legend, and a 1 h /
  6 h / 24 h range: commands per second, memory and clients per pod, and
  each replica's replication lag (which replaces the raw offset chart).
- **New cluster** (`/new`): a form for the name, namespace, build (standard,
  lua, tls, full), replicas, storage, memory and CPU, storage class, master
  service type, backups and monitoring. The manifest beside it is the API
  server's dry run of the form, updated as you type, so a mistake shows
  before anything is created; it can be copied for `kubectl apply`. Picking
  a build sets the matching image as well as `spec.variant` (the operator
  never derives one from the other). Creating needs GUI authentication, as
  every other change does. `POST /api/clusters` (`?dryRun=1` for the
  preview) and `GET /api/create-options` back it.
- **Light and dark themes**, following the system setting until you pick
  one; a layout for phones and tablets.
- **Logs**: a line count, follow mode, highlighting, and errors and warnings
  in colour. **Console** (the RESP explorer): replies printed as
  `redis-cli` prints them, quoted arguments, and history on ↑ / ↓.
- `GET /api/session`: whether the GUI accepts changes. On a read-only GUI
  every button that would only be refused is disabled, and says why.

### Changed

- Confirmations are in-page dialogs, not the browser's `confirm` and
  `prompt`; deleting a cluster asks for its name. Results show as toasts.
- The cluster page's tabs are Overview, Metrics, Logs, Console, Backups,
  and Events & operations.
- The GUI's ClusterRole (chart and `config/gui/rbac.yaml`) adds `create` on
  `kividbclusters` and `get`/`list` on `storageclasses`, for the new-cluster
  form.
- Default agent image **`docker.io/kividb/kividb-operator-agent:0.6.0`**.
  A `KividbCluster` that leaves `spec.agentImage` unset rolls its pods to
  it, one by one, when the operator is upgraded.
- The chart is signed with cosign, and Artifact Hub's repository metadata
  is published with it (from #8).

## [0.5.2] - 2026-10-24

A security release. The default engine stays kividb **1.1.2**.

### Security

- `github.com/klauspost/compress` v1.17.9 -> **v1.18.7**
  (CVE-2026-63209, GHSA-259r-337f-4rfw: an integer overflow in
  `s2.NewDict`). It reaches the agent through `minio-go`, which the agent
  uses for S3 backups. govulncheck finds no call to the vulnerable code.
- `golang.org/x/net` v0.56.0 -> **v0.60.0** and `golang.org/x/crypto`
  v0.53.0 -> **v0.57.0**, which fix the advisories govulncheck reports for
  them (none of the vulnerable code is called either), with `x/sys`,
  `x/term` and `x/text` moved along.

### Changed

- **Go 1.26.** `x/net` v0.60.0 and `x/crypto` v0.57.0 need it, so `go.mod`
  now says `go 1.26.0` and CI builds with 1.26, the same version as the
  release images (`golang:1.26-bookworm`). Building the operator from
  source needs Go 1.26.
- Default agent image **`docker.io/kividb/kividb-operator-agent:0.5.2`**.
  A `KividbCluster` that leaves `spec.agentImage` unset rolls its pods to
  it, one by one, when the operator is upgraded.

## [0.5.1] - 2026-10-24

The default engine is kividb **1.1.2**, a patch release of kividb 1.1.0
that fixes a `RESTORE` that could crash the server and expired keys piling
up under a steady load of writes with a TTL (kividb 1.1.1 was withdrawn for
the second). Validated on Azure Kubernetes Service (three nodes) with this
operator's code against kividb 1.1.2's: compat variants (standard, TLS,
Lua, full), a master killed under write load, snapshot chaos, monitoring,
RESP/ACL/SELECT, function libraries through a replica restart, a failover
and a restore, backup to S3 and restore, a sustained load of
`SET ... EX 5` on a master and two replicas, and in-place upgrades from
kividb 1.1.0 and 1.1.1 to 1.1.2 under write load (no password, ACL users,
TLS). No acknowledged write was lost in any of them.

### Changed

- **Default engine `docker.io/kividb/kividb:v1.1.2`** (it was `v1.1.0`):
  in the controller, the CRD, the chart, the GUI, the samples and the docs.
  **A `KividbCluster` that leaves `spec.image` unset is upgraded to 1.1.2
  when the operator is upgraded**: the operator resolves the default on
  every reconcile, so the pods roll one by one as for any image change. Set
  `spec.image` to stay on a version. kividb's image is distroless since
  1.1.2 (no shell, no `redis-cli` in the container); the operator only
  talks to kividb over RESP, through its agent, so nothing changes for it.
- Default agent image **`docker.io/kividb/kividb-operator-agent:0.5.1`**.
  A cluster that leaves `spec.agentImage` unset rolls to it too.
- The e2e suite tests kividb 1.1.2 by default.

### Fixed

- The e2e suite ran `kubectl exec ... redis-cli` and read `/proc` inside the
  kividb container, which the distroless image can't do. It now checks the
  TLS listener and runs `redis-cli` through a port-forward (#5).
- The docs named the backup CronJob `<cluster>-snapshot`; it is
  `<cluster>-backup` (#5).

## [0.5.0] - 2026-10-17

Validated on Azure Kubernetes Service (three nodes) against kividb v1.1.0:
a cluster without a password, a password-protected cluster with ACL users
upgraded in place from v1.0.5 to v1.1.0 under write load, a master killed
under write load, and the TLS build with a client CA. No acknowledged write
was lost in any of them.

### Changed

- **Default engine `docker.io/kividb/kividb:v1.1.0`.** Images now come
  from Docker Hub (`docker.io/kividb/...`) and GHCR (`ghcr.io/kividbio/...`),
  with the same tags in both; nothing is published to Quay any more. The
  operator, agent and GUI images, the chart, the CRD defaults, the samples
  and the docs point at Docker Hub. The Helm chart is at
  `oci://registry-1.docker.io/kividb/kividb-operator-chart` and
  `oci://ghcr.io/kividbio/kividb-operator-chart`.
- Default agent image **`docker.io/kividb/kividb-operator-agent:0.5.0`**.
- The release attaches the CRDs (`config/crd/bases/*.yaml`) as well as the
  packaged chart, as `docs/RELEASING.md` already told users to apply them
  from there.

### Fixed

- **A cluster without a password served nobody on kividb v1.1.0.**
  kividb 1.1.0 enforces protected mode: with no password on the `default`
  user and no `bind`, it refuses every client that is not on the pod's
  loopback, the other pods' replication included. The agent, on loopback,
  still reported the pods healthy, so such a cluster showed `Running` while
  every client and replica got `DENIED`. The operator now sets
  `bind 0.0.0.0`, the address kividb always listened on, unless you
  configure `bind` or `protected-mode` yourself. Existing clusters are
  rolled onto the new configuration by the operator upgrade.
- **The chart's `agent.image` was not used.** Every cluster without
  `spec.agentImage` pulled the image built into the operator, so mirroring
  the images to a private registry through the chart values left the
  sidecars unpullable. The chart now passes `agent.image` to the manager's
  new `--default-agent-image` flag.
- **The TLS port was on no Service.** The pods listened on it, but the
  master, replica and headless Services carried only the kividb and agent
  ports, so TLS clients could reach kividb only by pod IP. With TLS on, all
  three now carry it, named `tls`.

### Upgrade notes

- **kividb v1.1.0 with a TLS CA requires client certificates.** With
  `tls.certSecretRef.caKey` set, a TLS client without a certificate signed
  by that CA is refused; earlier engines let it in. Add the directive
  `tls-auth-clients: optional` (or `"no"`) to keep accepting it.
- Upgrade the operator first (its chart and CRDs), then change
  `spec.image`: the operator rolls the pods replicas first, then fails the
  master over, as for any engine upgrade.

## [0.4.0] - 2026-10-03

### Added

- **`KividbDbOps` CRD** with `op: restart` / `method: InPlace` rolling
  restart (replicas first, then master). GUI can create/list these.
- **`spec.bootstrapFromSnapshot`** on `KividbCluster`: seed a **new**
  cluster's pod-0 PVC from a Succeeded `KividbSnapshot` before the
  StatefulSet starts (supported restore path; see
  `docs/BACKUP_RESTORE.md`).
- Agent **`POST /exec`** for authenticated RESP commands (used by the GUI
  explorer).
- Agent **`restore-from-s3`** subcommand used by the bootstrap Job.
- Management GUI: Basic auth (`GUI_AUTH_*` / chart `gui.auth.existingSecret`),
  live status gauges, pod logs, RESP explorer, DbOps restart actions,
  24h local metrics scraper (PVC-backed) with sparkline charts.
- e2e suite **`08-resp-acl-select.sh`**: RESP3 `HELLO 3`, ACL deny smoke,
  `SELECT` on replica, failover single-master re-point. Override
  `KIVIDB_VERSION` (e.g. `v1.0.4-rc2`) for pre-GA engine testing.

### Changed

- Default engine pin **`quay.io/kividbio/kividb:v1.0.5`** (and docs/
  samples). Default agent image **`…-agent:0.4.0`**. v1.0.5 fixes all eleven
  engine issues recorded in `docs/KIVIDB_ENGINE_ISSUES.md`, among them
  unauthenticated access as the `default` user and the data-loss paths
  around failed resyncs and skipped shutdown snapshots.
- The agent sets `masterauth` to the `default` user's password before
  every `REPLICAOF`. kividb v1.0.5 refuses an unauthenticated replication
  handshake, so without it replicas of a password-protected cluster never
  sync. It is set at runtime, so images of older engines keep working.
- GUI ClusterRole gains `create` on `kividbdbops` and `get` on `pods/log`
  (still never Secrets to the browser).
- **Rolling updates are now done by the operator.** The StatefulSet uses
  the `OnDelete` update strategy and the operator replaces pods itself:
  replicas first, the master last, one at a time, and only while every
  pod is Ready and every replica has finished syncing. A pod that is
  unready on an outdated template is replaced without waiting.
- **The master is never restarted in place.** A rolling update or
  `KividbDbOps` restart first hands the master role to an in-sync replica
  (event `Switchover`) and then replaces the old master. Restarting it in
  place made every replica resync from whatever the master had last saved,
  which on EKS lost 55 seconds of acknowledged writes during a config
  change. Measured after the change: about one second of failed writes
  and none lost.
- **Failover will not promote an empty replica** when the master was last
  seen holding data: kividb drops a replica's dataset when a full resync
  fails part-way, which can empty every replica at once if the master is
  crash-looping. The failover waits for the master instead (event
  `FailoverBlocked`); set the `kividb.io/allow-empty-failover: "true"`
  annotation on the `KividbCluster` to fail over regardless.
  `status.pods[].keys` reports each pod's key count.
- `status.pods[].synced` reports whether a replica has completed its full
  sync from the current master. `KividbDbOps` restarts wait for it, and
  failover prefers replicas that were in sync.

### Fixed

- **Failover could hand the cluster back to the old master and lose
  writes.** The pod being failed away from kept its `role=master` label;
  when it came back it rejoined the master Service, was picked as the
  master again, and the pod that had been promoted was made its replica.
  The old master is now relabeled before its replacement is promoted and
  rejoins as a replica. `status.phase` also no longer sticks at
  `FailingOver` with `lastFailoverTime` advancing every reconcile.
- **A master that had been turned into a replica was never repaired**
  (cluster reported `Running`, every write failed with `READONLY`). The
  operator now follows the pod it is replicating from if that is a healthy
  master of the same cluster, and otherwise promotes it again.
- **`bootstrapFromSnapshot` produced an empty cluster.** The restored
  files were readable only by the restore Job's own user, so kividb could
  not load them and started empty while the bootstrap reported success.
- **`KividbDbOps` restart deleted every pod at once.** A pod counted as
  restarted while the deleted one was still terminating. Each pod must now
  be replaced, Ready and back in the cluster before the next is deleted.
- **Backups failed with `archive/tar: write too long`** whenever kividb
  appended to `appendonly.aof` during the upload.
- **ACL changes were never applied to running pods.** The operator now
  has each pod run `ACL LOAD` once the updated file has reached it.
- **`KividbConfig` changes were never applied to running pods.** They now
  roll the pods, as does a change of the `default` user's password.
- GUI: write endpoints (RESP explorer, restart, scale, promote, snapshot,
  delete) were open when no auth Secret was configured, and accepted
  cross-site requests. Promote sent `REPLICAOF` to backup Job pods, failed
  halfway and could leave the cluster without a master.
- The operator crash-looped if the `KividbDbOps` CRD was not installed,
  which is the case after a `helm upgrade` from 0.3.0.
- An unparseable `spec.storage.size` made the reconciler panic in a loop;
  it is now rejected by the CRD schema and reported on the cluster's
  `Ready` condition.
- The snapshot-restore pod was listed in `status.pods` as a cluster
  member, and pod-0's volume was the only one deleted with the cluster.
- A rolling update required the pod being replaced to be in sync as well
  as every other pod. Upgrading the engine from v1.0.4 to v1.0.5 stalled on
  the last old pod: once the master role was on a v1.0.5 pod, the v1.0.4
  pod could not authenticate to it and never became "synced".
- **A crash-looping pod blocked every rollout**, including the template
  change meant to fix it, and **rollouts moved on to the master while the
  replaced replica was still resyncing.** See "Changed" below.
- The GUI pod stayed `Pending` on clusters without a default StorageClass
  (EKS): its metrics PVC was on by default and could not be provisioned.
  `gui.metrics.persistence.enabled` now defaults to `false`.
- Upgrading the operator while pods still ran kividb v1.0.3 never started
  the rollout, because that engine does not report a replica's master.
- The agent reported every replica's master port as 0, which made the
  operator re-send `REPLICAOF` to every replica on every reconcile.

### Upgrade notes

- Apply the CRDs before upgrading the chart
  (`kubectl apply -f charts/kividb-operator/crds/`): `KividbDbOps` is new
  and `KividbCluster` gained validation. Helm does not do this for you.
- **Every existing cluster's pods are rolled once** after the operator
  is upgraded, one at a time: the pod template gains the agent's ACL
  mount and the `kividb.io/config-hash` / `kividb.io/auth-generation`
  annotations.
- The GUI refuses all writes until `gui.auth.existingSecret` is set.
- **Clusters without `spec.image` move to kividb v1.0.5** in that same
  rolling restart (from v1.0.3 if they were created by 0.3.0, or v1.0.4).
  Pin `spec.image` first to upgrade the engine separately. v1.0.5 changes
  behaviour that worked before:
  - **The `default` user's password is now enforced.** Clients that never
    sent `AUTH` (which worked, wrongly, on older engines) now get
    `NOAUTH`. Check client configuration before upgrading.
  - **Unknown or malformed directives in a `KividbConfig` are fatal**: the
    pod will not start. Older engines ignored them silently.
  - **`maxmemory` is enforced**, with `noeviction` by default: writes over
    the limit are refused.
- Clusters pinned to an engine older than v1.0.5 keep the old behaviour,
  including unauthenticated access as the `default` user; restrict
  network access to their Services.

### Notes

- Known engine issue in v1.0.5: a replica whose CPU limit is changed in
  place (for example by the Vertical Pod Autoscaler) panics on its next
  full resync and stops replicating until restarted. The operator itself
  never resizes pods in place. See `docs/KIVIDB_ENGINE_ISSUES.md`, issue 12.
- Out of scope: `ReducedImpact` restart, in-place restore of a live
  cluster.

## [0.3.0] - 2026-08-02

### Added

- Multi-arch release images (`linux/amd64` + `linux/arm64`) for the
  manager, agent, and GUI. 0.2.0 published an OCI index with only amd64
  (plus an attestation stub), which caused `ErrImagePull` /
  `no matching manifest for linux/arm64/v8` on Apple Silicon minikube and
  other arm64 nodes. Release and CI Docker builds now use buildx with
  QEMU and `platforms: linux/amd64,linux/arm64` (`provenance: false` so
  the index is not polluted with unknown/unknown attestation-only
  entries that confused some pullers).
- Unit tests for core pure functions: `electReplica`, `computePhase`,
  `renderKividbConf`, `renderACLFile`, naming helpers, and
  `desiredBackupCronJob` (`go test ./... -race`).
- Minikube e2e harness under `hack/e2e/` covering operator deploy,
  kube-prometheus-stack, MinIO-backed snapshots, variant/TLS compat
  against kividb `v1.0.3`, failover under load, snapshot chaos (pod kill
  mid-backup), and agent memory metrics under load (`make e2e`).

### Changed

- Samples, chart examples, and docs pin / recommend kividb **v1.0.3**
  (and `v1.0.3-tls` / `-lua` / `-full` variants) as the validated engine
  line for this operator release. When `spec.image` is unset, the
  operator now defaults to `quay.io/kividbio/kividb:v1.0.3` (no longer
  the floating `:latest` tag). Default `spec.agentImage` is
  `quay.io/kividbio/kividb-operator-agent:0.3.0`.
- CI Go version bumped to 1.25 to match `go.mod` / Dockerfiles.
- Dockerfiles honor `TARGETOS`/`TARGETARCH` from buildx so multi-arch
  compiles target the correct GOARCH.

### Compatibility (kividb 1.0.3)

- **TLS / Lua variants work on v1.0.3** (`v1.0.3-tls`, `v1.0.3-lua`,
  `v1.0.3-full`): e2e confirmed TLS port `LISTEN` and `EVAL` success.
  Prefer these tags over v1.0.2 for TLS.
- Remaining upstream caveats (configfile `tls-*` drop, ACL negative
  command rules, unauthenticated `PING`, replication RDB bulk-header)
  still tracked in ROADMAP — re-check with `hack/e2e/`.
- Snapshot robustness under source-pod or Job-pod kill is exercised by
  `hack/e2e/06-snapshot-chaos.sh` (no resume of an in-flight
  BGSAVE/upload; next manual/scheduled run should succeed after
  recovery).

## [0.2.0] - 2026-07-24

### Changed (breaking)

- Split the single `KividbCluster` CRD into five (`kividb.io/v1alpha1`):
  `KividbCluster` (topology, storage, scheduling, Services, monitoring,
  failover), `KividbConfig` (reusable `kividb.conf` directives + TLS
  settings), `KividbAclConfig` (reusable ACL users / `requirepass`),
  `KividbSnapshotConfig` (reusable backup schedule/destination/retention),
  and `KividbSnapshot` (operator-created record of one backup run). A
  StackGres-style split: configuration/ACLs/backup destinations are now
  independent, reusable objects referenced by name
  (`spec.configRef`/`spec.aclConfigRef`/`spec.snapshotConfigRef`) instead
  of embedded fields. `spec.auth`, `spec.backup`, and `spec.kividbConfig`
  no longer exist on `KividbCluster`.
- `spec.image` is now the sole field determining which kividb image runs.
  Removed `spec.version` and the operator's automatic
  `quay.io/kividbio/kividb:v<version>[-<variant>]` tag construction — the
  operator never derives or modifies an image reference itself anymore.
  Leave `spec.image` unset for a floating, unpinned default tag; set it
  explicitly to pin a version.
- `spec.variant` (`standard`/`tls`/`lua`/`full`) is now purely
  informational: it tells the operator whether to wire up variant-specific
  pod configuration (TLS cert mounting/CLI flags), but no longer
  influences image resolution. A likely `spec.image`/`spec.variant`
  mismatch, or TLS enabled in a `KividbConfig` without a matching variant,
  now surfaces as a guidance Event (`VariantGuidance`/
  `TLSVariantMismatch`) via `kubectl describe kividbcluster` rather than
  being silently wrong.
- `KividbCluster.status.backup` removed — backup history now lives on
  `KividbSnapshot` objects (`kubectl get kdbs -l kividb.io/cluster=<name>`),
  which carry per-run detail (source pod/role, object key, size, duration)
  the old single status block couldn't.

### Added

- `KividbSnapshotConfig.spec.source: master|replica` to choose which
  role's pod a scheduled backup is taken from (`master` is the default
  and recommended choice).
- `values.schema.json` for the Helm chart.
- Guidance Events (see above) surfaced on `KividbCluster` reconcile.
- Extensive rewrite of all docs for the new CRD architecture, plus
  `docs/ROADMAP.md` (0.2.0/1.0.0 planning and known upstream kividb
  issues found via live testing) and `docs/VERSIONING.md` (how an
  external site fetches these docs at a specific released version via
  git refs).

### Fixed

- TLS settings (`tls-port`/`tls-cert-file`/`tls-key-file`/
  `tls-ca-cert-file`) are now also passed to the `kividb` container as CLI
  flags, not only written into the rendered `kividb.conf` — live testing
  found kividb's `--configfile` parser currently ignores these directives
  when file-sourced, even though the identical CLI flags are documented
  and accepted.

### Security

- Bumped the builder image for all three published images
  (`kividb-operator`, `kividb-operator-agent`, `kividb-operator-gui`) from
  `golang:1.23-bookworm` to `golang:1.26-bookworm`, and bumped
  `golang.org/x/net` (0.28.0→0.56.0), `golang.org/x/oauth2`
  (0.21.0→0.27.0), `golang.org/x/sys` (0.24.0→0.46.0), and
  `golang.org/x/text` (0.17.0→0.39.0) — addressing a batch of stdlib and
  dependency CVEs (including a critical `crypto/tls` session-resumption
  issue, CVE-2025-68121) reported against the `0.1.0` images.

## [0.1.0] - 2026-07-24

### Added

- Initial release: `KividbCluster` CRD (`kividb.io/v1alpha1`) and
  controller managing a single-master, N-replica kividb StatefulSet.
- Automatic failover: master health is monitored via the per-pod agent
  sidecar; an unhealthy master is replaced by promoting the most
  caught-up replica and relabeling pods (`kividb.io/role`), with no
  Service object changes required.
- Separate master/replica Services, each configurable independently
  (`ClusterIP`/`NodePort`/`LoadBalancer`, static LB IP, source ranges,
  annotations).
- ACL user management (`spec.auth.users`) and legacy `requirepass`
  support, rendered into kividb's Redis-compatible ACL file format.
- Scheduled snapshot backups to any S3-compatible object storage
  (`spec.backup`), implemented as a native Kubernetes CronJob that
  triggers the current master's agent sidecar over HTTP; retention
  pruning included.
- Prometheus metrics: the agent sidecar's always-on lightweight `/metrics`
  (derived from kividb's `INFO` output), plus an optional third
  `redis-exporter` sidecar (`oliver006/redis_exporter`) added whenever
  `spec.monitoring.enabled: true`, and optional cluster-wide
  `ServiceMonitor` generation scraping both.
- Standard scheduling knobs: `resources`, `tolerations`, `nodeSelector`,
  `affinity` (with a sensible preferred-anti-affinity default).
- A read-only web GUI (`cmd/gui`) for cluster/pod status.
- Helm chart (`charts/kividb-operator`) and kustomize bases
  (`config/`) for installation.

[Unreleased]: https://github.com/kividbio/kividb-operator/compare/v0.7.1...master
[0.7.1]: https://github.com/kividbio/kividb-operator/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/kividbio/kividb-operator/compare/v0.6.1...v0.7.0
[0.6.1]: https://github.com/kividbio/kividb-operator/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/kividbio/kividb-operator/compare/v0.5.2...v0.6.0
[0.5.2]: https://github.com/kividbio/kividb-operator/compare/v0.5.1...v0.5.2
[0.5.1]: https://github.com/kividbio/kividb-operator/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/kividbio/kividb-operator/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/kividbio/kividb-operator/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/kividbio/kividb-operator/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/kividbio/kividb-operator/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/kividbio/kividb-operator/releases/tag/v0.1.0
