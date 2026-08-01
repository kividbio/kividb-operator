# Releasing

## Versioning

kividb-operator follows [Semantic Versioning](https://semver.org/). One
version number covers everything released together:

- the operator manager image (`quay.io/kividbio/kividb-operator`)
- the agent sidecar image (`quay.io/kividbio/kividb-operator-agent`)
- the GUI image (`quay.io/kividbio/kividb-operator-gui`)
- the Helm chart (`charts/kividb-operator`, both `version` and
  `appVersion` in `Chart.yaml`)
- the `VERSION` file at the repo root

All five are always bumped together and tagged identically — there is no
independent versioning between the manager/agent/GUI images or between
the chart and the app version. This keeps "which agent image works with
which operator version" a non-question: they're always released in
lockstep and the CRD's `spec.agentImage` default always points at the
matching tag.

The CRD's API version (`kividb.io/v1alpha1`) is versioned independently
and much more conservatively than the release version above — a `v1beta1`
or `v1` would only be cut alongside a deliberate, documented migration
path for existing `KividbCluster` objects, not on every release.

## One-time setup: GitHub secrets for publishing

`release.yaml` pushes to `quay.io/kividbio/*` using a **Quay.io robot
account** (not `GITHUB_TOKEN` — unlike GHCR, Quay.io isn't tied to your
GitHub identity). Before the first tag push:

1. In quay.io, under the `kividbio` organization: create a robot account
   (e.g. `kividbio+github_actions`) and grant it **Write** access to the
   `kividb-operator`, `kividb-operator-agent`, and `kividb-operator-gui`
   repositories (Quay creates image repositories automatically on first
   push, but the robot account still needs write permission granted
   either org-wide or per-repo once they exist).
2. In the GitHub repo's Settings → Secrets and variables → Actions, add:
   - `QUAY_USERNAME` — the robot account's full name, e.g.
     `kividbio+github_actions`
   - `QUAY_PASSWORD` — the robot account's token (shown once when created;
     regenerate from quay.io if lost)
3. New quay.io repositories default to a visibility your org has
   configured (often private) — if you want anonymous `docker pull` /
   `helm pull` to work without credentials, mark each of the three image
   repositories and the chart repository public in quay.io after their
   first push.

## Publishing to ArtifactHub

ArtifactHub doesn't host charts — it's a catalog that indexes a chart
already published elsewhere. Once the chart has been pushed to
`oci://quay.io/kividbio/kividb-operator-chart` at least once (i.e. after
the first tag/release):

1. Sign in at [artifacthub.io](https://artifacthub.io) with the account
   that should own the listing.
2. "Add repository" → kind **Helm charts (OCI)** → repository URL
   `oci://quay.io/kividbio/kividb-operator-chart` → name it
   `kividb-operator`.
3. ArtifactHub will periodically re-scan that OCI path for new chart
   versions — no webhook or CI step is needed for *updates* once the
   repository is registered, only for this one-time registration.
4. Optional polish: `charts/kividb-operator/Chart.yaml` already carries
   an `artifacthub.io/...` annotations block (icon, links, maintainers) —
   keep it in sync with reality as those change.

## What CI checks on every push/PR (`.github/workflows/ci.yaml`)

- `go mod tidy` (must produce no diff — run it locally before pushing if
  you added/removed an import)
- `go vet ./...`
- `gofmt -l .` (must report nothing)
- `go test ./... -race`
- `helm lint charts/kividb-operator`
- Docker build (not push) of all three images, to catch Dockerfile
  breakage early

None of this ran successfully in the environment this repo was
originally scaffolded in (no network access, so no Go module cache) — the
**first real CI run against this repo is the first real correctness
check** for the Go code. Treat a red first CI run as expected-until-proven
otherwise, not a regression.

## Cutting a release

1. Update `CHANGELOG.md`: move `[Unreleased]` content under a new
   `## [X.Y.Z] - YYYY-MM-DD` heading.
2. Bump the version in three places to match:
   - `VERSION` (repo root)
   - `charts/kividb-operator/Chart.yaml` (`version:` and `appVersion:`)
   - `internal/controller/names.go` `DefaultAgentImage` (and chart
     `values.yaml` image tags)
3. If this release added, removed, or renamed a doc under `docs/`, update
   `docs/manifest.json` to match (see [VERSIONING.md](VERSIONING.md) —
   most releases don't touch this, since docs are edited in place, not
   duplicated per version).
4. Confirm multi-arch: `.github/workflows/release.yaml` must build
   `platforms: linux/amd64,linux/arm64` (0.2.0 shipped amd64-only and
   broke Apple Silicon pulls — do not regress this).
5. Commit: `git commit -m "Release vX.Y.Z"`.
6. Tag and push: `git tag vX.Y.Z && git push origin main --tags`.
7. `.github/workflows/release.yaml` triggers on the `v*` tag push and:
   - builds and pushes all three images to `quay.io/kividbio/*`, tagged
     both `X.Y.Z` and `latest`, for **amd64 and arm64**,
   - packages the Helm chart (`helm package charts/kividb-operator`,
     producing `kividb-operator-chart-X.Y.Z.tgz` -- the package name
     comes from `Chart.yaml`'s `name:`, which is intentionally
     `kividb-operator-chart`, not `kividb-operator`, to avoid colliding
     with the image repo of the same name; see the comment in
     `.github/workflows/release.yaml`) and pushes it as an OCI artifact
     to `oci://quay.io/kividbio/kividb-operator-chart`,
   - creates a GitHub Release for the tag, using the corresponding
     `CHANGELOG.md` section as the release body, with the packaged
     chart `.tgz` attached as a release asset.
8. Verify the release (from both amd64 and arm64 if you can):

   ```bash
   docker pull quay.io/kividbio/kividb-operator:X.Y.Z
   docker manifest inspect quay.io/kividbio/kividb-operator:X.Y.Z
   helm show chart oci://quay.io/kividbio/kividb-operator-chart --version X.Y.Z
   ```

## Upgrading an existing installation

**Helm does not upgrade CRDs on `helm upgrade`** (this is a deliberate
Helm 3 design choice — see `charts/kividb-operator/crds/` in
[INSTALL.md](INSTALL.md)). Every release that changes the CRD schema
needs an explicit:

```bash
kubectl apply -f https://github.com/kividbio/kividb-operator/releases/download/vX.Y.Z/kividb.io_kividbclusters.yaml
```

(or `kubectl apply -k config/crd` from a checked-out tag) **before**
running `helm upgrade`. Check the release notes for whether a given
version actually changed the CRD — if it didn't, this step is a no-op.

## Pre-release checklist

- [ ] `CHANGELOG.md` updated
- [ ] `VERSION` and `Chart.yaml` bumped together
- [ ] `docs/manifest.json` still matches `docs/*.md` if any doc files
      were added/removed/renamed this release
- [ ] CI green on the release commit
- [ ] `config/samples/*.yaml` still apply cleanly against the new CRD
- [ ] If the CRD schema changed: called out explicitly in the changelog
      entry, with the `kubectl apply` step above mentioned
- [ ] Multi-arch images: after the release workflow finishes, confirm
      `docker manifest inspect quay.io/kividbio/kividb-operator:X.Y.Z`
      lists both `linux/amd64` and `linux/arm64`
- [ ] `make test` (unit) green; optionally `make e2e` against minikube
      with `LOAD_IMAGES=1` before tagging
- [ ] If `spec.agentImage`'s default changed: double-checked
      `internal/controller/names.go`'s `DefaultAgentImage` constant
      matches the tag actually being pushed
