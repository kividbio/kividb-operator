#!/usr/bin/env bash
# 01-deploy-operator.sh — install/upgrade the Helm chart with local image tags.
#
# Mirrors a typical Dragonfly-operator style deploy: CRDs + manager Deployment
# + optional GUI, with the agent image tag published for per-cluster use
# (agent is a sidecar on each KividbCluster StatefulSet, not a chart workload).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

log "=== deploy operator (tag=${OPERATOR_TAG}) ==="
require kubectl helm

ensure_ns "${OPERATOR_NS}"

# CRDs: apply explicitly so upgrades pick up schema changes (Helm installs
# charts/crds/ only on first install and never upgrades them).
log "applying CRDs from ${CHART_DIR}/crds"
kubectl apply -f "${CHART_DIR}/crds"

if [[ "${LOAD_IMAGES}" == "1" ]]; then
  require minikube docker
  for img in "${MANAGER_IMG}" "${AGENT_IMG}" "${GUI_IMG}"; do
    log "minikube image load ${img}"
    if docker image inspect "${img}" >/dev/null 2>&1; then
      minikube image load "${img}"
    else
      warn "local docker image ${img} not found — skipping load (pull or docker-build first)"
    fi
  done
fi

# ServiceMonitor requires monitoring.coreos.com CRDs (installed by
# 02-prometheus.sh). Enable when present; otherwise install without it and
# let 02-prometheus.sh helm-upgrade the chart to turn it on.
SM_ENABLED=false
if kubectl get crd servicemonitors.monitoring.coreos.com >/dev/null 2>&1; then
  SM_ENABLED=true
else
  warn "ServiceMonitor CRD missing — deploying with metrics.serviceMonitor.enabled=false (02-prometheus.sh will enable it)"
fi

log "helm upgrade --install kividb-operator (serviceMonitor=${SM_ENABLED})"
helm upgrade --install kividb-operator "${CHART_DIR}" \
  --namespace "${OPERATOR_NS}" \
  --create-namespace \
  --set "manager.image.repository=${OPERATOR_REGISTRY}/kividb-operator" \
  --set "manager.image.tag=${OPERATOR_TAG}" \
  --set "manager.image.pullPolicy=IfNotPresent" \
  --set "agent.image.repository=${OPERATOR_REGISTRY}/kividb-operator-agent" \
  --set "agent.image.tag=${OPERATOR_TAG}" \
  --set "agent.image.pullPolicy=IfNotPresent" \
  --set "gui.enabled=true" \
  --set "gui.image.repository=${OPERATOR_REGISTRY}/kividb-operator-gui" \
  --set "gui.image.tag=${OPERATOR_TAG}" \
  --set "gui.image.pullPolicy=IfNotPresent" \
  --set "metrics.serviceMonitor.enabled=${SM_ENABLED}" \
  --set "manager.resources.requests.cpu=50m" \
  --set "manager.resources.requests.memory=64Mi" \
  --set "manager.resources.limits.cpu=250m" \
  --set "manager.resources.limits.memory=256Mi" \
  --set "gui.resources.requests.cpu=25m" \
  --set "gui.resources.requests.memory=32Mi" \
  --wait --timeout 180s

wait_pods_ready "${OPERATOR_NS}" "app.kubernetes.io/name=kividb-operator" 180
log "operator deployed in ${OPERATOR_NS}"
