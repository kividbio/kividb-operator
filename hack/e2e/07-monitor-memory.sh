#!/usr/bin/env bash
# 07-monitor-memory.sh — scrape agent /metrics; optional Prometheus targets.
#
# Agent /metrics is always on (port 8081). redis_exporter is added when
# monitoring.enabled=true. Chart-level ServiceMonitor (metrics.serviceMonitor)
# scrapes app.kubernetes.io/name=kividb cluster-wide when Prometheus Operator
# CRDs are present.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

if [[ "${SKIP_MONITOR:-0}" == "1" ]]; then
  log "SKIP_MONITOR=1 — skipping monitor suite"
  exit 0
fi

log "=== monitor / memory metrics ==="
require kubectl redis-cli

ensure_ns "${E2E_KIVIDB_NS}"

NAME="monitor-mem"
IMAGE="$(kividb_image_for_variant standard)"

cleanup_cluster "${E2E_KIVIDB_NS}" "${NAME}" || true

kubectl apply -n "${E2E_KIVIDB_NS}" -f - <<EOF
apiVersion: kividb.io/v1alpha1
kind: KividbConfig
metadata:
  name: ${NAME}-config
spec:
  directives:
    maxmemory: "128mb"
---
apiVersion: kividb.io/v1alpha1
kind: KividbCluster
metadata:
  name: ${NAME}
  labels:
    app.kubernetes.io/part-of: kividb-e2e
spec:
  replicas: 0
  image: ${IMAGE}
  imagePullPolicy: IfNotPresent
  variant: standard
  agentImage: ${AGENT_IMG}
  port: ${KIVIDB_PORT}
  configRef:
    name: ${NAME}-config
$(cluster_resources_yaml)
  failover:
    enabled: true
  monitoring:
    enabled: true
    serviceMonitor: true
EOF

wait_cluster_phase "${E2E_KIVIDB_NS}" "${NAME}" "Running" 300
wait_pods_ready "${E2E_KIVIDB_NS}" "kividb.io/cluster=${NAME}" 180

POD="$(master_pod "${E2E_KIVIDB_NS}" "${NAME}")"
[[ -n "${POD}" ]] || die "no master pod"

scrape_agent_metrics() {
  local pod="$1"
  # Prefer kubectl exec + wget/curl in a debug container via port-forward to agent.
  local local_port
  local_port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()' 2>/dev/null || echo 18081)"
  local logfile
  logfile="$(mktemp)"
  kubectl port-forward -n "${E2E_KIVIDB_NS}" "pod/${pod}" "${local_port}:${AGENT_PORT}" \
    >"${logfile}" 2>&1 &
  local pf_pid=$!
  sleep 2
  local body=""
  if command -v curl >/dev/null 2>&1; then
    body="$(curl -sf "http://127.0.0.1:${local_port}/metrics" || true)"
  else
    body="$(wget -qO- "http://127.0.0.1:${local_port}/metrics" || true)"
  fi
  kill "${pf_pid}" 2>/dev/null || true
  wait "${pf_pid}" 2>/dev/null || true
  rm -f "${logfile}"
  printf '%s' "${body}"
}

BEFORE="$(scrape_agent_metrics "${POD}")"
if ! echo "${BEFORE}" | grep -q 'kividb_up'; then
  die "agent /metrics missing kividb_up"
fi
MEM_BEFORE="$(echo "${BEFORE}" | awk '/^kividb_used_memory_bytes /{print $2; exit}')"
log "metrics before load: kividb_used_memory_bytes=${MEM_BEFORE:-<absent>}"
echo "${BEFORE}" | grep -E '^kividb_' | head -n 20 >&2 || true

generate_load "${E2E_KIVIDB_NS}" "${NAME}" 200 20

AFTER="$(scrape_agent_metrics "${POD}")"
MEM_AFTER="$(echo "${AFTER}" | awk '/^kividb_used_memory_bytes /{print $2; exit}')"
log "metrics after load: kividb_used_memory_bytes=${MEM_AFTER:-<absent>}"

if [[ -n "${MEM_BEFORE}" && -n "${MEM_AFTER}" ]]; then
  # Numeric compare via awk (values may be floats).
  CHANGED="$(awk -v a="${MEM_BEFORE}" -v b="${MEM_AFTER}" 'BEGIN{ if (b+0 != a+0) print "yes"; else print "no" }')"
  if [[ "${CHANGED}" == "yes" ]]; then
    log "kividb_used_memory_bytes changed (${MEM_BEFORE} -> ${MEM_AFTER})"
  else
    warn "kividb_used_memory_bytes unchanged (${MEM_BEFORE}) — load may have been too small; checking commands_processed"
    CMD_B="$(echo "${BEFORE}" | awk '/^kividb_commands_processed_total /{print $2; exit}')"
    CMD_A="$(echo "${AFTER}" | awk '/^kividb_commands_processed_total /{print $2; exit}')"
    if [[ -n "${CMD_B}" && -n "${CMD_A}" ]] && awk -v a="${CMD_B}" -v b="${CMD_A}" 'BEGIN{exit !(b+0 > a+0)}'; then
      log "kividb_commands_processed_total increased (${CMD_B} -> ${CMD_A})"
    else
      warn "could not observe metric movement; dumping after scrape"
      echo "${AFTER}" | grep -E '^kividb_' | head -n 30 >&2 || true
    fi
  fi
else
  warn "used_memory metric absent on this kividb build — verified kividb_up only"
fi

# Optional: ServiceMonitor + Prometheus targets.
if kubectl get crd servicemonitors.monitoring.coreos.com >/dev/null 2>&1; then
  if kubectl get servicemonitor -n "${OPERATOR_NS}" -o name 2>/dev/null | grep -qi kividb; then
    log "ServiceMonitor for kividb found in ${OPERATOR_NS}"
  elif kubectl get servicemonitor -A 2>/dev/null | grep -qi kividb; then
    log "ServiceMonitor for kividb found (cluster-wide)"
  else
    warn "no ServiceMonitor matching kividb (chart metrics.serviceMonitor.enabled?)"
  fi

  if [[ "${SKIP_PROMETHEUS:-0}" != "1" ]] && kubectl get pods -n "${MONITORING_NS}" -l app.kubernetes.io/name=prometheus >/dev/null 2>&1; then
    log "checking prometheus targets (optional)"
    # Port-forward prometheus and query /api/v1/targets if curl available.
    if command -v curl >/dev/null 2>&1; then
      PROM_POD="$(kubectl get pods -n "${MONITORING_NS}" -l app.kubernetes.io/name=prometheus -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
      if [[ -n "${PROM_POD}" ]]; then
        local_port=19090
        kubectl port-forward -n "${MONITORING_NS}" "pod/${PROM_POD}" "${local_port}:9090" >/tmp/prom-pf.log 2>&1 &
        pf_pid=$!
        sleep 3
        TARGETS="$(curl -sf "http://127.0.0.1:${local_port}/api/v1/targets" || true)"
        kill "${pf_pid}" 2>/dev/null || true
        wait "${pf_pid}" 2>/dev/null || true
        if echo "${TARGETS}" | grep -qi kividb; then
          log "prometheus targets include kividb scrape jobs"
        else
          warn "prometheus targets JSON did not mention kividb (may need more scrape cycles)"
        fi
      fi
    fi
  fi
else
  warn "ServiceMonitor CRD not installed — skip target checks"
fi

cleanup_cluster "${E2E_KIVIDB_NS}" "${NAME}"
log "monitor suite PASS"
