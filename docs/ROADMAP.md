# Roadmap

This tracks what's deliberately deferred, not a promise of exact timing.
Items are grouped by target release; "1.0.0" lists what needs to be true
before that version number is used at all (see
[RELEASING.md](RELEASING.md) for what 1.0.0 signals and why v0.1.x
shouldn't skip straight there).

## Known upstream kividb issues

### Re-verified against kividb v1.0.3 (operator 0.3.0 e2e, 2026-08-02)

- **TLS listener on `-tls`/`-full`:** **works on v1.0.3.** Operator e2e
  (`hack/e2e/04-compat-variants.sh`) confirmed `/proc/net/tcp` shows the
  configured `tls-port` (default 6443) in `LISTEN` for both
  `v1.0.3-tls` and `v1.0.3-full`, with plaintext `PING` still healthy.
  Prefer pinning `spec.image` to those tags and `variant: tls`/`full`
  with a `KividbConfig` that enables TLS.
- **Lua on `-lua`/`-full`:** `EVAL` succeeds on v1.0.3.

### Still open / previously confirmed on v1.0.2 (2026-07-23)

These were found running the multi-CRD architecture end-to-end on a real
EKS cluster. They're kividb binary/protocol issues, not bugs in this
operator's reconciliation logic — re-check on each engine bump.

- **`-tls`/`-full` on v1.0.2 did not open the TLS listener** (fixed by
  v1.0.3 per above). Historical detail: neither conf directives nor CLI
  flags produced a listening TLS socket on 1.0.2.
- **kividb's `--configfile` parser silently drops `tls-*` directives.**
  Independent of the listener: hand-edited `kividb.conf` files still
  should not be trusted for TLS — only identically-named CLI flags are
  reliable. The operator sends both.
- **Per-command ACL denies are accepted but not enforced** (as of the
  1.0.2 live test). An ACL rule like `+@all -flushall -flushdb` is
  echoed by `ACL GETUSER` but `FLUSHALL` still executes. Re-verify on
  newer engines before treating negative `commandRules` as an
  enforcement boundary.
- **`PING` does not require authentication even with a password set**
  (as of 1.0.2). Low severity, but don't use unauthenticated `PING` as a
  security signal.
- **Replication RDB bulk-header bug** (see 0.2.0 notes below) — replica
  reads may still be unreliable until fixed upstream.

None of the remaining items are blocked on this operator — they're
upstream kividb fixes. This operator's job is to keep configuring things
correctly and keep this list current as kividb's behavior changes.

## 0.2.0 (shipped)

Items below that landed in 0.2.0 stay listed for history; open follow-ups
moved under 0.3.0 / Before 1.0.0.

- **Unit and integration tests.** Every fix documented in this repo's
  commit history so far was found by manual live testing against a real
  EKS cluster, not by an automated suite -- there isn't one yet. Priority
  order: pure functions in `internal/controller` (config/ACL rendering,
  `computePhase`, `electReplica`) with `go test`, then an `envtest`-based
  suite for the reconciler itself.
- **Restore automation.** `docs/BACKUP_RESTORE.md` documents a fully
  manual restore procedure today. A `KividbRestore` CRD (mirroring
  `KividbSnapshot`'s design: user creates it naming a source
  `KividbSnapshot`, the operator handles scale-down, PVC population, and
  scale-up) would close this gap the same way `KividbSnapshot` closed the
  "we never captured the object key" gap.
- **kividb replication bug.** A real upstream bug was found live: the
  master sends something other than the expected `$<len>\r\n` RDB bulk
  header immediately after `+FULLRESYNC`, so replicas complete the
  handshake but never actually receive data (see the kividb project's own
  issue tracker, not this repo). Until it's fixed upstream, replica reads
  in this operator are not reliable -- this should be called out
  prominently in end-user docs until resolved.
- **`KividbSnapshot.status.objectKey` scrape-free confidence.** The
  current implementation reads the backup-trigger container's termination
  message, which is solid but has one edge: if kubelet ever truncates a
  very large termination message (there's a size cap, historically
  4096 bytes) the JSON could fail to parse. `BackupResult` today is small
  enough this shouldn't happen in practice, but worth a defensive size
  check + truncation warning rather than a silent parse failure.
- **GUI: browse the new CRDs.** The dashboard shows `KividbCluster` status
  (now including its `KividbSnapshot` history) but has no list/detail view
  for `KividbConfig`/`KividbAclConfig`/`KividbSnapshotConfig` themselves --
  useful once clusters start sharing them.
- **`KividbConfig`/`KividbAclConfig`/`KividbSnapshotConfig` usage
  tracking.** No `status.usedBy` on these three -- deleting one out from
  under a referencing `KividbCluster` currently just surfaces as a
  reconcile error on the cluster (visible, but not proactively guarded
  against, e.g. no admission webhook blocking the delete).
- **Chart signing.** `values.schema.json` is shipped; GPG-signed
  provenance (`helm package --sign`) is documented in RELEASING.md but not
  wired into the release pipeline -- needs a real, verifiable signing
  identity decided by the project, not a throwaway key.
- **ArtifactHub "official" status**, if desired -- an organizational
  decision (who publishes as kividb's own maintainers), not an engineering
  task.

## 0.3.0 (shipped / in progress)

- **Multi-arch operator images** (`linux/amd64` + `linux/arm64`) — fixes
  the Apple Silicon / arm64 `ErrImagePull` on the 0.2.0 Quay tags.
- **Automated tests:** unit coverage for election/phase/config/ACL/
  backup CronJob rendering, plus a minikube e2e suite
  (`hack/e2e/`, `make e2e`) for variants vs kividb v1.0.3, failover under
  load, snapshot chaos (pod kill mid-backup), and Prometheus/memory
  scrape under load.
- **Engine pin:** samples and docs target kividb **v1.0.3** (+ variant
  tags). Upstream TLS / ACL / replication caveats from the 2026-07-23
  live test are re-verified by e2e rather than assumed fixed.

## 0.4.0 and beyond (less firm)

- **`KividbCluster` horizontal read scaling helpers** -- e.g. a
  `spec.replicas` autoscaling hook driven by replica CPU/connection count,
  rather than always-manual `replicas`.
- **Multi-cluster / cross-region replication** -- explicitly out of scope
  for the current architecture (see ARCHITECTURE.md's "what this operator
  deliberately does not do").
- **Admission webhooks** for stricter validation than the CRD schema alone
  can express (e.g. "if `spec.variant` is `tls`, `configRef` must point at
  a `KividbConfig` with `spec.tls.enabled: true`"). Deferred because it
  adds a cert-manager (or self-signed cert rotation) dependency to
  installation, which the project has otherwise deliberately avoided.
- **PVC online expansion automation** when `spec.storage.size` changes
  (currently manual -- see TROUBLESHOOTING.md).

## Before 1.0.0

Semver 1.0.0 is a deliberate milestone, not just "whatever's next after
0.x". Before using it:

1. Real automated test coverage exists (see 0.2.0 above) -- 1.0.0 without
   this would just be a version number, not a signal.
2. The CRD API (five kinds now: `KividbCluster`, `KividbConfig`,
   `KividbAclConfig`, `KividbSnapshotConfig`, `KividbSnapshot`) has held
   stable across at least one full 0.x release without a breaking field
   change. It changed twice in one day during initial development
   (0.1.0's original design, then this multi-CRD split) -- that pace needs
   to visibly settle first.
3. The kividb upstream replication bug above is fixed, or explicitly
   documented as a known, permanent limitation rather than an open bug.
4. At least one restore has been exercised end-to-end (manual is fine, but
   it needs to have actually happened, not just be theoretically
   documented).
