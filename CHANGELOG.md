# Changelog

All notable changes to kividb-operator are documented in this file. The
format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and versioning follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Fixed

- **Replication with kividb v1.0.5 and a `default`-user password.** From
  v1.0.5 the engine enforces that password, including on the replication
  handshake, so replicas never synced. The agent now sets `masterauth` to
  the default user's password before every `REPLICAOF` (at runtime, so
  images of older engines, which have no such setting, keep working).
- **Rolling updates no longer require the pod being replaced to be in
  sync**, only every other pod. Upgrading the engine from v1.0.4 to v1.0.5
  otherwise stalled on the last old pod: after the master role moved to a
  v1.0.5 pod, the v1.0.4 pod could not authenticate to it, never became
  "synced", and so was never replaced.

## [0.4.0] - 2026-08-18

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

- Default engine pin **`quay.io/kividbio/kividb:v1.0.4`** (and docs/
  samples). Default agent image **`…-agent:0.4.0`**.
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
- **A `default`-user password does not stop unauthenticated clients on
  kividb v1.0.4.** This is an engine issue the operator cannot work
  around; see `docs/KIVIDB_ENGINE_ISSUES.md` and restrict network access
  to the cluster's Services.

### Notes

- Engine ACL caveats (implicit default auth when `requirepass` empty;
  last-wins multi `keyPatterns`) remain documented in ROADMAP — Cloud-safe;
  not fixed in this operator release.
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
