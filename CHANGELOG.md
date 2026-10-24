# Changelog

All notable changes to kividb-operator are documented in this file. The
format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and versioning follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

**Read before upgrading:** the agent's port (8081) is no longer on the
`<cluster>-master` and `<cluster>-replicas` Services, and the agent's API
needs a token. Anything that called an agent through those Services, other
than the operator's own backup Jobs, has to change. The pods roll once.

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

### Changed

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

### Fixed

- The chart's ServiceMonitor scraped each pod twice, through the headless
  Service and through its master or replica Service. It selects the
  headless Service only (`kividb.io/service: headless`).

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

[Unreleased]: https://github.com/kividbio/kividb-operator/compare/v0.4.0...main
[0.4.0]: https://github.com/kividbio/kividb-operator/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/kividbio/kividb-operator/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/kividbio/kividb-operator/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/kividbio/kividb-operator/releases/tag/v0.1.0
