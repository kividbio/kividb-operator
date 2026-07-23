# Roadmap

This tracks what's deliberately deferred, not a promise of exact timing.
Items are grouped by target release; "1.0.0" lists what needs to be true
before that version number is used at all (see
[RELEASING.md](RELEASING.md) for what 1.0.0 signals and why v0.1.x
shouldn't skip straight there).

## Known upstream kividb issues (confirmed by live testing, 2026-07-23)

These were found running the multi-CRD architecture end-to-end on a real
EKS cluster for the first time. They're kividb binary/protocol issues,
not bugs in this operator's reconciliation logic — the operator is
producing exactly the configuration it's designed to, but kividb doesn't
act on (or, in the TLS case, doesn't yet implement) parts of it. Listed
here because they materially affect what you should rely on today.

- **`-tls`/`-full` variant images do not open the TLS listener at all.**
  Verified two ways: neither writing `tls-port`/`tls-cert-file`/
  `tls-key-file` into `kividb.conf` (the original design) nor passing the
  identical settings as `--tls-port`/`--tls-cert-file`/`--tls-key-file`
  CLI flags (which the image's own `--help` documents and which this
  operator now also does, in [statefulset.go](../internal/controller/statefulset.go),
  as a belt-and-suspenders fix for the config-file gap below) results in
  a listening socket on the configured port — confirmed by reading
  `/proc/net/tcp` inside the running container: only the plaintext port
  ever appears in `LISTEN` state. **Until kividb itself implements this,
  `spec.variant: tls`/`full` and `KividbConfig.spec.tls` have no effect
  beyond selecting a different image tag and mounting certs that go
  unused.** Don't rely on this for anything security-sensitive today.
- **kividb's `--configfile` parser silently drops `tls-*` directives.**
  Independent of the listener issue above: even once kividb does
  implement the listener, anyone hand-editing a `kividb.conf`-style file
  (or reading the operator's rendered ConfigMap for reference) should
  know the file-based path doesn't currently work — only identically-named
  CLI flags do. The operator now sends both, so this is transparent to
  `KividbCluster` users; it only matters if you're comparing the rendered
  ConfigMap against actual runtime behavior.
- **Per-command ACL denies are accepted but not enforced.** An ACL rule
  like `+@all -flushall -flushdb` is parsed and echoed back correctly by
  `ACL GETUSER` — kividb's own view of the rule is right — but the denied
  command still executes. Verified live: an `app` user configured with
  exactly that rule set successfully ran `FLUSHALL`. This means
  `KividbAclConfig`'s `commandRules` negative rules (anything starting
  with `-`) should not be trusted as an enforcement boundary today; only
  `keyPatterns`/`channelPatterns` restriction and category-level
  (`+@read` etc.) positive grants were not tested to the same depth this
  pass and warrant re-verification before relying on them either.
- **`PING` does not require authentication even with a password set.**
  Real Redis requires `AUTH` before any command except `AUTH`/`HELLO`/
  `RESET`/`QUIT` once `requirepass` is set; kividb currently allows `PING`
  through unauthenticated. Low severity on its own, but worth knowing if
  you're using unauthenticated `PING` reachability as any kind of signal.

None of the above are blocked on this operator — they're upstream kividb
fixes. This operator's job here is to keep configuring things correctly
(which live testing now confirms it does) and to keep this list current
as kividb's own behavior changes.

## 0.2.0

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

## 0.3.0 and beyond (less firm)

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
