#!/usr/bin/env bash
# Shared helpers for the minikube e2e/compat harness.
#
# Layout intentionally echoes patterns from StackGres (separate Config CRDs
# applied alongside the cluster) and the Dragonfly operator (StatefulSet +
# sidecar agent): scripts apply KividbConfig / KividbSnapshotConfig first,
# then KividbCluster, and talk to the agent sidecar over HTTP / redis-cli
# against the master Service — never by assuming StatefulSet ordinal 0 is
# always the master.
#
# shellcheck shell=bash
set -euo pipefail

# Resolve repo root from this file's location (hack/e2e/lib.sh).
_E2E_SRC="${BASH_SOURCE[0]:-$0}"
E2E_DIR="$(cd "$(dirname "${_E2E_SRC}")" && pwd)"
ROOT_DIR="$(cd "${E2E_DIR}/../.." && pwd)"
CHART_DIR="${ROOT_DIR}/charts/kividb-operator"

# ---------------------------------------------------------------------------
# Defaults (overridable via environment)
# ---------------------------------------------------------------------------
OPERATOR_NS="${OPERATOR_NS:-kividb-operator-system}"
E2E_NS="${E2E_NS:-e2e-ns}"
E2E_KIVIDB_NS="${E2E_KIVIDB_NS:-e2e-kividb}"
MONITORING_NS="${MONITORING_NS:-monitoring}"

OPERATOR_TAG="${OPERATOR_TAG:-0.3.0-local}"
OPERATOR_REGISTRY="${OPERATOR_REGISTRY:-quay.io/kividbio}"
MANAGER_IMG="${MANAGER_IMG:-${OPERATOR_REGISTRY}/kividb-operator:${OPERATOR_TAG}}"
AGENT_IMG="${AGENT_IMG:-${OPERATOR_REGISTRY}/kividb-operator-agent:${OPERATOR_TAG}}"
GUI_IMG="${GUI_IMG:-${OPERATOR_REGISTRY}/kividb-operator-gui:${OPERATOR_TAG}}"

KIVIDB_VERSION="${KIVIDB_VERSION:-v1.0.3}"
KIVIDB_IMAGE_BASE="${KIVIDB_IMAGE_BASE:-quay.io/kividbio/kividb}"
KIVIDB_PORT="${KIVIDB_PORT:-6380}"
TLS_PORT="${TLS_PORT:-6443}"
AGENT_PORT="${AGENT_PORT:-8081}"

STORAGE_CLASS="${STORAGE_CLASS:-standard}"
PVC_SIZE="${PVC_SIZE:-1Gi}"

# MinIO (local e2e only — never commit real credentials)
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_BUCKET="${MINIO_BUCKET:-kividb-e2e-backups}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio.${E2E_NS}.svc.cluster.local:9000}"

STRICT_TLS="${STRICT_TLS:-0}"
LOAD_IMAGES="${LOAD_IMAGES:-0}"

RESULTS_DIR="${RESULTS_DIR:-${E2E_DIR}/results}"

# Tiny resource requests suitable for minikube.
E2E_CPU_REQUEST="${E2E_CPU_REQUEST:-50m}"
E2E_MEM_REQUEST="${E2E_MEM_REQUEST:-64Mi}"
E2E_CPU_LIMIT="${E2E_CPU_LIMIT:-250m}"
E2E_MEM_LIMIT="${E2E_MEM_LIMIT:-256Mi}"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
log() {
  printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*" >&2
}

die() {
  log "ERROR: $*"
  exit 1
}

warn() {
  log "WARN: $*"
}

# ---------------------------------------------------------------------------
# Prerequisites
# ---------------------------------------------------------------------------
require() {
  local missing=0
  local cmd
  for cmd in "$@"; do
    if ! command -v "${cmd}" >/dev/null 2>&1; then
      log "missing required command: ${cmd}"
      missing=1
    fi
  done
  if [[ "${missing}" -ne 0 ]]; then
    die "install the missing command(s) and re-run"
  fi
}

# ---------------------------------------------------------------------------
# Wait helpers
# ---------------------------------------------------------------------------
# wait_for_condition SECONDS description -- condition_cmd...
wait_for_condition() {
  local timeout="$1"
  local description="$2"
  shift 2
  local start now
  start="$(date +%s)"
  log "waiting up to ${timeout}s for: ${description}"
  while true; do
    if "$@"; then
      log "ready: ${description}"
      return 0
    fi
    now="$(date +%s)"
    if (( now - start >= timeout )); then
      die "timed out after ${timeout}s waiting for: ${description}"
    fi
    sleep 2
  done
}

# wait_cluster_phase NS NAME PHASE TIMEOUT
wait_cluster_phase() {
  local ns="$1" name="$2" phase="$3" timeout="$4"
  wait_for_condition "${timeout}" "KividbCluster/${name} phase=${phase}" \
    bash -c "[[ \"\$(kubectl get kividbcluster -n '${ns}' '${name}' -o jsonpath='{.status.phase}' 2>/dev/null)\" == '${phase}' ]]"
}

# wait_pods_ready NS LABEL TIMEOUT
# Ignores Succeeded/Failed pods so Completed backup Jobs that share
# kividb.io/cluster=... do not block readiness forever.
wait_pods_ready() {
  local ns="$1" label="$2" timeout="$3"
  wait_for_condition "${timeout}" "pods ready (-l ${label} in ${ns})" \
    bash -c '
      ns="$1"; label="$2"
      # Prefer StatefulSet members only when label is a bare cluster selector.
      sel="$label"
      if [[ "$label" == kividb.io/cluster=* && "$label" != *app.kubernetes.io/name=* ]]; then
        sel="app.kubernetes.io/name=kividb,${label}"
      fi
      count=$(kubectl get pods -n "$ns" -l "$sel" --field-selector=status.phase=Running -o jsonpath="{.items[*].metadata.name}" 2>/dev/null | wc -w | tr -d " ")
      [[ "${count:-0}" -ge 1 ]] || exit 1
      names=$(kubectl get pods -n "$ns" -l "$sel" --field-selector=status.phase=Running -o jsonpath="{.items[*].metadata.name}" 2>/dev/null || true)
      for p in $names; do
        ready=$(kubectl get pod -n "$ns" "$p" -o jsonpath="{.status.conditions[?(@.type==\"Ready\")].status}" 2>/dev/null || true)
        [[ "$ready" == "True" ]] || exit 1
      done
    ' bash "${ns}" "${label}"
}

# wait_snapshot_succeeded NS CLUSTER TIMEOUT
wait_snapshot_succeeded() {
  local ns="$1" cluster="$2" timeout="$3"
  wait_for_condition "${timeout}" "Succeeded KividbSnapshot for cluster=${cluster}" \
    bash -c "
      kubectl get kdbs -n '${ns}' -l 'kividb.io/cluster=${cluster}' \
        -o jsonpath='{range .items[?(@.status.phase==\"Succeeded\")]}{.metadata.name}{\"\\n\"}{end}' 2>/dev/null \
        | grep -q .
    "
}

# ---------------------------------------------------------------------------
# Cluster helpers
# ---------------------------------------------------------------------------
master_pod() {
  local ns="$1" cluster="$2"
  kubectl get pods -n "${ns}" \
    -l "kividb.io/cluster=${cluster},kividb.io/role=master" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null
}

# redis_cli_master NS CLUSTER_NAME args...
# Uses local redis-cli via port-forward to the master Service (role-based,
# Dragonfly-style — not StatefulSet ordinal 0).
redis_cli_master() {
  local ns="$1" cluster="$2"
  shift 2
  require redis-cli
  local local_port logfile pf_pid ready i rc
  local_port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()' 2>/dev/null || echo $((16000 + RANDOM % 2000)))"
  logfile="$(mktemp)"
  kubectl port-forward -n "${ns}" "svc/${cluster}-master" "${local_port}:${KIVIDB_PORT}" \
    >"${logfile}" 2>&1 &
  pf_pid=$!
  ready=0
  for i in $(seq 1 30); do
    if redis-cli -h 127.0.0.1 -p "${local_port}" PING 2>/dev/null | grep -qi pong; then
      ready=1
      break
    fi
    sleep 0.5
  done
  if [[ "${ready}" -ne 1 ]]; then
    kill "${pf_pid}" 2>/dev/null || true
    cat "${logfile}" >&2 || true
    rm -f "${logfile}"
    die "port-forward to ${cluster}-master failed"
  fi
  rm -f "${logfile}"
  rc=0
  redis-cli -h 127.0.0.1 -p "${local_port}" "$@" || rc=$?
  kill "${pf_pid}" 2>/dev/null || true
  wait "${pf_pid}" 2>/dev/null || true
  return "${rc}"
}

# generate_load NS CLUSTER_NAME KEYS DURATION
generate_load() {
  local ns="$1" cluster="$2" keys="$3" duration="$4"
  log "generating load: keys=${keys} duration≈${duration}s against ${cluster}"
  require redis-cli
  local local_port logfile pf_pid ready i end n
  local_port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()' 2>/dev/null || echo $((16000 + RANDOM % 2000)))"
  logfile="$(mktemp)"
  kubectl port-forward -n "${ns}" "svc/${cluster}-master" "${local_port}:${KIVIDB_PORT}" \
    >"${logfile}" 2>&1 &
  pf_pid=$!
  ready=0
  for i in $(seq 1 30); do
    if redis-cli -h 127.0.0.1 -p "${local_port}" PING 2>/dev/null | grep -qi pong; then
      ready=1
      break
    fi
    sleep 0.5
  done
  if [[ "${ready}" -ne 1 ]]; then
    kill "${pf_pid}" 2>/dev/null || true
    cat "${logfile}" >&2 || true
    rm -f "${logfile}"
    die "port-forward to ${cluster}-master failed (load)"
  fi
  rm -f "${logfile}"

  if command -v redis-benchmark >/dev/null 2>&1; then
    n=$(( keys * 20 ))
    redis-benchmark -h 127.0.0.1 -p "${local_port}" -n "${n}" -c 4 -t set,get -q || true
  else
    i=0
    end=$(( $(date +%s) + duration ))
    while (( $(date +%s) < end )) && (( i < keys * 10 )); do
      redis-cli -h 127.0.0.1 -p "${local_port}" SET "e2e:load:${i}" "v${i}" >/dev/null || true
      redis-cli -h 127.0.0.1 -p "${local_port}" GET "e2e:load:${i}" >/dev/null || true
      i=$((i + 1))
    done
    log "load loop completed (${i} ops)"
  fi

  kill "${pf_pid}" 2>/dev/null || true
  wait "${pf_pid}" 2>/dev/null || true
}

# kill_pod NS POD — force delete with no grace period (chaos helper).
kill_pod() {
  local ns="$1" pod="$2"
  log "force-deleting pod ${ns}/${pod}"
  kubectl delete pod -n "${ns}" "${pod}" --force --grace-period=0 --wait=false
}

ensure_ns() {
  local ns="$1"
  kubectl get ns "${ns}" >/dev/null 2>&1 || kubectl create ns "${ns}"
}

kividb_image_for_variant() {
  local variant="$1"
  case "${variant}" in
    standard) echo "${KIVIDB_IMAGE_BASE}:${KIVIDB_VERSION}" ;;
    tls)      echo "${KIVIDB_IMAGE_BASE}:${KIVIDB_VERSION}-tls" ;;
    lua)      echo "${KIVIDB_IMAGE_BASE}:${KIVIDB_VERSION}-lua" ;;
    full)     echo "${KIVIDB_IMAGE_BASE}:${KIVIDB_VERSION}-full" ;;
    *) die "unknown variant: ${variant}" ;;
  esac
}

port_to_hex() {
  printf '%04X' "$1"
}

# check_tls_listen NS POD PORT
# Returns 0 if /proc/net/tcp shows LISTEN (st=0A) on PORT in the kividb container.
check_tls_listen() {
  local ns="$1" pod="$2" port="$3"
  local hex
  hex="$(port_to_hex "${port}")"
  kubectl exec -n "${ns}" "${pod}" -c kividb -- \
    sh -c "grep -i ':${hex}' /proc/net/tcp /proc/net/tcp6 2>/dev/null | grep -qi ' 0A '" \
    >/dev/null 2>&1
}

has_event_reason() {
  local ns="$1" kind="$2" name="$3" reason="$4"
  kubectl get events -n "${ns}" \
    --field-selector "involvedObject.kind=${kind},involvedObject.name=${name},reason=${reason}" \
    -o jsonpath='{.items[*].reason}' 2>/dev/null | grep -q "${reason}"
}

cluster_resources_yaml() {
  cat <<EOF
  storage:
    size: ${PVC_SIZE}
    storageClassName: ${STORAGE_CLASS}
    accessModes:
      - ReadWriteOnce
  resources:
    requests:
      cpu: ${E2E_CPU_REQUEST}
      memory: ${E2E_MEM_REQUEST}
    limits:
      cpu: ${E2E_CPU_LIMIT}
      memory: ${E2E_MEM_LIMIT}
  agentResources:
    requests:
      cpu: 25m
      memory: 32Mi
    limits:
      cpu: 100m
      memory: 64Mi
  services:
    master:
      type: ClusterIP
    replicas:
      type: ClusterIP
EOF
}

ensure_tls_secret() {
  local ns="$1" name="$2"
  if kubectl get secret -n "${ns}" "${name}" >/dev/null 2>&1; then
    return 0
  fi
  require openssl
  local tmp
  tmp="$(mktemp -d)"
  openssl req -x509 -nodes -newkey rsa:2048 \
    -keyout "${tmp}/tls.key" -out "${tmp}/tls.crt" \
    -days 1 -subj "/CN=kividb-e2e" >/dev/null 2>&1
  kubectl create secret tls "${name}" -n "${ns}" \
    --cert="${tmp}/tls.crt" --key="${tmp}/tls.key"
  rm -rf "${tmp}"
}

cleanup_cluster() {
  local ns="$1" name="$2"
  log "cleaning up KividbCluster/${name} in ${ns}"
  kubectl delete kividbcluster -n "${ns}" "${name}" --ignore-not-found --wait=true --timeout=180s || true
  kubectl delete kividbconfig -n "${ns}" "${name}-config" --ignore-not-found || true
  kubectl delete kividbsnapshotconfig -n "${ns}" "${name}-snap" --ignore-not-found || true
  kubectl delete secret -n "${ns}" "${name}-tls" --ignore-not-found || true
  kubectl delete secret -n "${ns}" "${name}-s3" --ignore-not-found || true
  kubectl delete kdbs -n "${ns}" -l "kividb.io/cluster=${name}" --ignore-not-found || true
  kubectl delete pvc -n "${ns}" -l "app.kubernetes.io/instance=${name}" --ignore-not-found || true
  sleep 2
}
