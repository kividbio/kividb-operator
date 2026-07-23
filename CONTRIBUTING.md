# Contributing

Thanks for considering a contribution to kividb-operator.

## Development setup

Requires Go 1.23+, Docker, `kubectl`, `helm`, and a local Kubernetes
cluster (kind, minikube, or Docker Desktop's built-in Kubernetes all
work).

```bash
git clone https://github.com/kividbio/kividb-operator.git
cd kividb-operator
go mod tidy
make fmt vet test
```

## Running against a local cluster

```bash
make install                # kubectl apply -k config/crd
go run . -leader-elect=false   # runs the manager locally, outside the cluster
```

In a second terminal, apply a sample and watch it converge:

```bash
kubectl apply -f config/samples/kividb.io_v1alpha1_kividbcluster_minimal.yaml
kubectl get kividbcluster -w
```

## Code layout

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the design. Briefly:

- `api/v1alpha1/` — CRD Go types. `zz_generated.deepcopy.go` is written by
  hand in this repo (no `controller-gen` was run when it was created); if
  you have `controller-gen` available, `make generate manifests` will
  regenerate both it and the CRD YAML mechanically instead — please
  regenerate rather than hand-editing further if you have the tool.
- `internal/controller/` — the reconciler.
- `internal/agentapi/` — the JSON contract between the controller and the
  agent sidecar. Treat changes here as a breaking-change consideration:
  the controller and agent are versioned and released together (see
  [docs/RELEASING.md](docs/RELEASING.md)), but a rolling upgrade will
  briefly run a new operator against old agent pods (and vice versa)
  until the StatefulSet rollout finishes.
- `internal/respclient/` — minimal RESP2 client (stdlib only, no Redis
  client library dependency).
- `cmd/agent/` — the sidecar binary.
- `cmd/gui/` — the read-only dashboard.

## Pull requests

- Run `make fmt vet test` before opening a PR; CI runs the same checks
  plus `helm lint`.
- Keep `docs/CONFIGURATION.md` in sync with any CRD field you add or
  change, and add/update a `config/samples/*.yaml` example if the change
  is user-facing.
- Add a `CHANGELOG.md` entry under `[Unreleased]`.
- If you change `internal/agentapi/types.go`, update both call sites
  (`internal/controller/agentclient.go` and `cmd/agent/server.go`) in the
  same PR — they're deliberately not decoupled behind an interface, so
  the compiler catches drift for you as long as both sides get touched
  together.

## Reporting issues

Please include `kubectl get kividbcluster <name> -o yaml`, relevant pod
events/logs, and operator logs — see
[docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) first in case it's a
known issue with a documented fix.
