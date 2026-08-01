# Changelog

All notable changes to kividb-operator are documented in this file. The
format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and versioning follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed

- Release and CI Docker builds use **native** multi-arch runners
  (`ubuntu-latest` for amd64, `ubuntu-24.04-arm` for arm64), push each
  arch by digest, then merge into one OCI index — replacing the previous
  QEMU-emulated `platforms: linux/amd64,linux/arm64` single-runner model
  that made arm64 release builds very slow.

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

[Unreleased]: https://github.com/kividbio/kividb-operator/compare/v0.3.0...main
[0.3.0]: https://github.com/kividbio/kividb-operator/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/kividbio/kividb-operator/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/kividbio/kividb-operator/releases/tag/v0.1.0
