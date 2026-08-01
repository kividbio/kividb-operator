#!/usr/bin/env bash
# 00-prereqs.sh — verify local tools and a running minikube cluster.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

log "=== e2e prereqs ==="
require kubectl helm minikube docker redis-cli

ARCH="$(uname -m)"
OS="$(uname -s)"
log "host arch=${ARCH} os=${OS}"
log "kubectl: $(kubectl version --client -o yaml 2>/dev/null | awk '/gitVersion/{print $2; exit}' || kubectl version --client --short 2>/dev/null || true)"
log "helm:    $(helm version --short 2>/dev/null || true)"
log "docker:  $(docker version --format '{{.Client.Version}}' 2>/dev/null || true)"
log "minikube:$(minikube version --short 2>/dev/null || true)"

if ! minikube status >/dev/null 2>&1; then
  die "minikube is not running — start with: minikube start --cpus=4 --memory=6144"
fi

# Align kubectl with the minikube context.
minikube update-context >/dev/null 2>&1 || true
CTX="$(kubectl config current-context 2>/dev/null || true)"
log "kubectl context: ${CTX:-<none>}"

# Prefer the minikube storage class name when present.
if kubectl get storageclass standard >/dev/null 2>&1; then
  :
elif kubectl get storageclass standard-rwo >/dev/null 2>&1; then
  warn "storage class 'standard' missing; set STORAGE_CLASS=standard-rwo if needed"
elif DEFAULT_SC="$(kubectl get storageclass -o jsonpath='{.items[?(@.metadata.annotations.storageclass\.kubernetes\.io/is-default-class=="true")].metadata.name}' 2>/dev/null)"; then
  if [[ -n "${DEFAULT_SC}" ]]; then
    warn "using default StorageClass '${DEFAULT_SC}' (override with STORAGE_CLASS=...)"
  fi
fi

log "OPERATOR_TAG=${OPERATOR_TAG} KIVIDB_VERSION=${KIVIDB_VERSION} LOAD_IMAGES=${LOAD_IMAGES} STRICT_TLS=${STRICT_TLS}"
log "images: manager=${MANAGER_IMG} agent=${AGENT_IMG} gui=${GUI_IMG}"
log "prereqs OK"
