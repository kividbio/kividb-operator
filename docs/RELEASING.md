# Releasing

## Versioning

kividb-operator follows [Semantic Versioning](https://semver.org/). One
version number covers everything released together:

- the operator manager image (`kividb-operator`)
- the agent sidecar image (`kividb-operator-agent`)
- the GUI image (`kividb-operator-gui`)
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

## Where releases go

Each image and the chart are published to two registries, with the same
tags, the way kividb itself is:

| | GHCR | Docker Hub |
|---|---|---|
| Images | `ghcr.io/kividbio/kividb-operator`, `-agent`, `-gui` | `docker.io/kividb/kividb-operator`, `-agent`, `-gui` |
| Chart | `oci://ghcr.io/kividbio/kividb-operator-chart` | `oci://registry-1.docker.io/kividb/kividb-operator-chart` |

The per-arch builds push to GHCR; the manifest job then creates the
multi-arch `X.Y.Z` and `latest` tags in both registries from those images.
The defaults in the chart, the CRD and the samples point at Docker Hub.

## One-time setup

1. **Docker Hub:** create the public repositories `kividb/kividb-operator`,
   `kividb/kividb-operator-agent`, `kividb/kividb-operator-gui` and
   `kividb/kividb-operator-chart`, and an access token with Read & Write on
   them. In this repo's Settings → Secrets and variables → Actions, set the
   variable `DOCKERHUB_USERNAME` (the account or organization that owns the
   token) and the secret `DOCKERHUB_TOKEN`. The release stops before
   building anything if either is missing.
2. **GHCR:** nothing to set up: the workflow pushes with `GITHUB_TOKEN`. A
   package is created private the first time it is pushed; make each of the
   four (`kividb-operator`, `-agent`, `-gui`, `-chart`) public once, in its
   package settings, or anonymous pulls from GHCR fail.

## Publishing to ArtifactHub

ArtifactHub doesn't host charts: it indexes the chart on GHCR,
`oci://ghcr.io/kividbio/kividb-operator-chart`, and scans it for new
versions by itself. The repository is registered once, under the `kividbio`
organization, with that URL and **no tag** (`...-chart:0.5.2` makes every
scan fail).

Its badges:

- **Verified Publisher:** `artifacthub-repo.yml` at the root of this repo
  holds the Artifact Hub repository ID. The `artifacthub` workflow pushes it
  into the chart's repository as the `artifacthub.io` tag whenever it
  changes on master (or by hand, from the Actions tab), and Artifact Hub
  verifies the repository at its next scan.
- **Signed:** the release workflow signs each chart version it pushes to
  GHCR with cosign, keyless, as this repository's release workflow. Check a
  signature with
  `cosign verify ghcr.io/kividbio/kividb-operator-chart:<version> --certificate-identity-regexp '^https://github.com/kividbio/kividb-operator/' --certificate-oidc-issuer https://token.actions.githubusercontent.com`.
- **Official:** asked for once the repository is verified, with an issue on
  [artifacthub/hub](https://github.com/artifacthub/hub/issues).
- Security rating: from Artifact Hub's own scan of the images the chart
  uses.

Keep the `artifacthub.io/...` annotations in
`charts/kividb-operator/Chart.yaml` (links, CRDs, examples) in step with
reality.

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
4. Confirm multi-arch: `.github/workflows/release.yaml` builds each
   arch **natively** (`ubuntu-latest` for amd64, `ubuntu-24.04-arm` for
   arm64), then merges digests into one OCI index — do **not** regress
   to QEMU/`platforms: linux/amd64,linux/arm64` on a single amd64 runner
   (that is how 0.2.0–0.3.0 release builds got painfully slow).
5. Commit: `git commit -m "Release vX.Y.Z"`.
6. Tag and push: `git tag vX.Y.Z && git push origin master --tags`.
7. `.github/workflows/release.yaml` triggers on the `v*` tag push and:
   - builds all three images for **amd64 and arm64** (native runners) and
     publishes them to GHCR and Docker Hub, tagged both `X.Y.Z` and
     `latest` (a manifest-merge job),
   - packages the Helm chart (`helm package charts/kividb-operator`,
     producing `kividb-operator-chart-X.Y.Z.tgz` -- the package name
     comes from `Chart.yaml`'s `name:`, which is intentionally
     `kividb-operator-chart`, not `kividb-operator`, to avoid colliding
     with the image repo of the same name; see the comment in
     `.github/workflows/release.yaml`) and pushes it as an OCI artifact
     to both registries,
   - creates a GitHub Release for the tag, using the corresponding
     `CHANGELOG.md` section as the release body, with the packaged
     chart `.tgz` and the CRDs (`config/crd/bases/*.yaml`) attached.
8. Verify the release (from both amd64 and arm64 if you can):

   ```bash
   docker pull docker.io/kividb/kividb-operator:X.Y.Z
   docker buildx imagetools inspect ghcr.io/kividbio/kividb-operator:X.Y.Z
   helm show chart oci://registry-1.docker.io/kividb/kividb-operator-chart --version X.Y.Z
   helm show chart oci://ghcr.io/kividbio/kividb-operator-chart --version X.Y.Z
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
      `docker manifest inspect docker.io/kividb/kividb-operator:X.Y.Z`
      lists both `linux/amd64` and `linux/arm64`
- [ ] `make test` (unit) green; optionally `make e2e` against minikube
      with `LOAD_IMAGES=1` before tagging
- [ ] If `spec.agentImage`'s default changed: double-checked
      `internal/controller/names.go`'s `DefaultAgentImage` constant
      matches the tag actually being pushed
