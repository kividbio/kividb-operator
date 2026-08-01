#!/usr/bin/env bash
# 02-prometheus.sh — install kube-prometheus-stack (minimal) for ServiceMonitor tests.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

if [[ "${SKIP_PROMETHEUS:-0}" == "1" ]]; then
  log "SKIP_PROMETHEUS=1 — skipping prometheus install"
  exit 0
fi

log "=== prometheus (kube-prometheus-stack) ==="
require kubectl helm

ensure_ns "${MONITORING_NS}"

if helm status kube-prometheus-stack -n "${MONITORING_NS}" >/dev/null 2>&1; then
  log "kube-prometheus-stack already installed in ${MONITORING_NS}"
else
  log "adding prometheus-community helm repo"
  helm repo add prometheus-community https://prometheus-community.github.io/helm-charts >/dev/null 2>&1 || true
  helm repo update prometheus-community >/dev/null

  # Minimal footprint for minikube: disable alertmanager/grafana/node-exporter/
  # kube-state-metrics extras where possible; tiny Prometheus resources.
  log "installing kube-prometheus-stack (minimal)"
  helm upgrade --install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
    --namespace "${MONITORING_NS}" \
    --create-namespace \
    --set prometheus.prometheusSpec.retention=6h \
    --set prometheus.prometheusSpec.resources.requests.cpu=100m \
    --set prometheus.prometheusSpec.resources.requests.memory=256Mi \
    --set prometheus.prometheusSpec.resources.limits.cpu=500m \
    --set prometheus.prometheusSpec.resources.limits.memory=512Mi \
    --set prometheus.prometheusSpec.storageSpec.volumeClaimTemplate.spec.resources.requests.storage=2Gi \
    --set alertmanager.enabled=false \
    --set grafana.enabled=false \
    --set kubeStateMetrics.enabled=false \
    --set nodeExporter.enabled=false \
    --set prometheusOperator.resources.requests.cpu=50m \
    --set prometheusOperator.resources.requests.memory=64Mi \
    --set prometheusOperator.resources.limits.cpu=200m \
    --set prometheusOperator.resources.limits.memory=128Mi \
    --wait --timeout 600s
fi

wait_for_condition 300 "prometheus Ready" \
  bash -c "
    kubectl get pods -n '${MONITORING_NS}' -l 'app.kubernetes.io/name=prometheus' \
      -o jsonpath='{.items[0].status.conditions[?(@.type==\"Ready\")].status}' 2>/dev/null | grep -qx True
  "

# Enable the chart ServiceMonitor now that monitoring.coreos.com CRDs exist.
if helm status kividb-operator -n "${OPERATOR_NS}" >/dev/null 2>&1; then
  log "enabling metrics.serviceMonitor on kividb-operator release"
  helm upgrade kividb-operator "${CHART_DIR}" \
    --namespace "${OPERATOR_NS}" \
    --reuse-values \
    --set "metrics.serviceMonitor.enabled=true" \
    --set "metrics.serviceMonitor.labels.release=kube-prometheus-stack" \
    --wait --timeout 120s || warn "helm upgrade for ServiceMonitor failed (non-fatal)"
fi

log "prometheus ready in ${MONITORING_NS}"
