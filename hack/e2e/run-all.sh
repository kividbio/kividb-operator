#!/usr/bin/env bash
# run-all.sh — sequential minikube e2e suite (00 → 07) with a summary report.
#
# Skip individual suites with SKIP_*=1, e.g.:
#   SKIP_PROMETHEUS=1 SKIP_MONITOR=1 ./hack/e2e/run-all.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

mkdir -p "${RESULTS_DIR}"
SUMMARY="${RESULTS_DIR}/latest.txt"
STAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

SUITES=(
  "00-prereqs.sh"
  "01-deploy-operator.sh"
  "02-prometheus.sh"
  "03-minio.sh"
  "04-compat-variants.sh"
  "05-failover-load.sh"
  "06-snapshot-chaos.sh"
  "07-monitor-memory.sh"
)

# Map suite → skip env var (empty = never skip via flag; always runs).
skip_var_for() {
  case "$1" in
    00-prereqs.sh) echo "" ;;
    01-deploy-operator.sh) echo "SKIP_DEPLOY" ;;
    02-prometheus.sh) echo "SKIP_PROMETHEUS" ;;
    03-minio.sh) echo "SKIP_MINIO" ;;
    04-compat-variants.sh) echo "SKIP_COMPAT" ;;
    05-failover-load.sh) echo "SKIP_FAILOVER" ;;
    06-snapshot-chaos.sh) echo "SKIP_SNAPSHOT" ;;
    07-monitor-memory.sh) echo "SKIP_MONITOR" ;;
    *) echo "" ;;
  esac
}

declare -a RESULTS=()
PASS=0
FAIL=0
SKIP=0

log "=== kividb-operator minikube e2e @ ${STAMP} ==="
log "OPERATOR_TAG=${OPERATOR_TAG} KIVIDB_VERSION=${KIVIDB_VERSION} STRICT_TLS=${STRICT_TLS} LOAD_IMAGES=${LOAD_IMAGES}"

for suite in "${SUITES[@]}"; do
  skip_var="$(skip_var_for "${suite}")"
  if [[ -n "${skip_var}" && "${!skip_var:-0}" == "1" ]]; then
    log "SKIP ${suite} (${skip_var}=1)"
    RESULTS+=("SKIP  ${suite}")
    SKIP=$((SKIP + 1))
    continue
  fi

  log "RUN  ${suite}"
  start="$(date +%s)"
  if bash "${SCRIPT_DIR}/${suite}"; then
    elapsed=$(( $(date +%s) - start ))
    RESULTS+=("PASS  ${suite} (${elapsed}s)")
    PASS=$((PASS + 1))
    log "PASS ${suite} (${elapsed}s)"
  else
    rc=$?
    elapsed=$(( $(date +%s) - start ))
    RESULTS+=("FAIL  ${suite} (rc=${rc}, ${elapsed}s)")
    FAIL=$((FAIL + 1))
    log "FAIL ${suite} (rc=${rc}, ${elapsed}s)"
    # Continue so later suites still contribute to the summary unless STOP_ON_FAIL=1.
    if [[ "${STOP_ON_FAIL:-0}" == "1" ]]; then
      break
    fi
  fi
done

{
  echo "kividb-operator minikube e2e summary"
  echo "timestamp: ${STAMP}"
  echo "OPERATOR_TAG=${OPERATOR_TAG}"
  echo "KIVIDB_VERSION=${KIVIDB_VERSION}"
  echo "STRICT_TLS=${STRICT_TLS}"
  echo "LOAD_IMAGES=${LOAD_IMAGES}"
  echo "host: $(uname -s)/$(uname -m)"
  echo "---"
  for line in "${RESULTS[@]}"; do
    echo "${line}"
  done
  echo "---"
  echo "totals: pass=${PASS} fail=${FAIL} skip=${SKIP}"
} | tee "${SUMMARY}"

log "summary written to ${SUMMARY}"

if (( FAIL > 0 )); then
  exit 1
fi
exit 0
