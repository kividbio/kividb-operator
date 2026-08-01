#!/usr/bin/env bash
# 05-failover-load.sh — kill the master under load; wait for role relabel.
#
# Dragonfly-operator style: failover is a role-label move on the StatefulSet
# pods (kividb.io/role), not a Service rewrite. Clients keep talking to
# <cluster>-master ClusterIP.
#
# Caveat (docs/ROADMAP.md): upstream kividb has a replication bug where
# replicas may complete the handshake but never receive data. After failover,
# "data presence" checks are best-effort — we verify writes still work on the
# new master and note when pre-failover keys are missing due to replica lag.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

if [[ "${SKIP_FAILOVER:-0}" == "1" ]]; then
  log "SKIP_FAILOVER=1 — skipping failover suite"
  exit 0
fi

log "=== failover under load ==="
require kubectl redis-cli

ensure_ns "${E2E_KIVIDB_NS}"

NAME="failover-load"
IMAGE="$(kividb_image_for_variant standard)"
# 1 master + 2 replicas => spec.replicas=2
REPLICAS=2

cleanup_cluster "${E2E_KIVIDB_NS}" "${NAME}" || true

kubectl apply -n "${E2E_KIVIDB_NS}" -f - <<EOF
apiVersion: kividb.io/v1alpha1
kind: KividbConfig
metadata:
  name: ${NAME}-config
spec:
  directives:
    maxmemory: "64mb"
    loglevel: "notice"
---
apiVersion: kividb.io/v1alpha1
kind: KividbCluster
metadata:
  name: ${NAME}
  labels:
    app.kubernetes.io/part-of: kividb-e2e
spec:
  replicas: ${REPLICAS}
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
    unhealthyThresholdSeconds: 10
  monitoring:
    enabled: false
EOF

wait_cluster_phase "${E2E_KIVIDB_NS}" "${NAME}" "Running" 360
wait_pods_ready "${E2E_KIVIDB_NS}" "kividb.io/cluster=${NAME}" 240

OLD_MASTER="$(master_pod "${E2E_KIVIDB_NS}" "${NAME}")"
[[ -n "${OLD_MASTER}" ]] || die "no master pod before failover"
log "current master: ${OLD_MASTER}"

# Seed keys before chaos.
SEED_KEYS=20
for i in $(seq 0 $((SEED_KEYS - 1))); do
  redis_cli_master "${E2E_KIVIDB_NS}" "${NAME}" SET "e2e:seed:${i}" "seed-${i}" >/dev/null
done
log "seeded ${SEED_KEYS} keys"

# Background load while we kill the master.
LOAD_PID=""
(
  generate_load "${E2E_KIVIDB_NS}" "${NAME}" 50 60
) &
LOAD_PID=$!
sleep 3

kill_pod "${E2E_KIVIDB_NS}" "${OLD_MASTER}"

# Wait for a different pod to acquire kividb.io/role=master.
wait_for_condition 180 "new master elected (≠ ${OLD_MASTER})" \
  bash -c '
    ns="$1"; cluster="$2"; old="$3"
    cur=$(kubectl get pods -n "$ns" -l "kividb.io/cluster=${cluster},kividb.io/role=master" \
      -o jsonpath="{.items[0].metadata.name}" 2>/dev/null || true)
    [[ -n "$cur" && "$cur" != "$old" ]]
  ' bash "${E2E_KIVIDB_NS}" "${NAME}" "${OLD_MASTER}"

NEW_MASTER="$(master_pod "${E2E_KIVIDB_NS}" "${NAME}")"
log "new master: ${NEW_MASTER}"

wait_cluster_phase "${E2E_KIVIDB_NS}" "${NAME}" "Running" 300

# Writes must work on the new master.
WRITE_OUT="$(redis_cli_master "${E2E_KIVIDB_NS}" "${NAME}" SET e2e:post-failover ok || true)"
if ! echo "${WRITE_OUT}" | grep -qi ok; then
  die "post-failover SET failed: ${WRITE_OUT}"
fi
READ_OUT="$(redis_cli_master "${E2E_KIVIDB_NS}" "${NAME}" GET e2e:post-failover || true)"
if [[ "${READ_OUT}" != "ok" ]]; then
  die "post-failover GET failed: ${READ_OUT}"
fi
log "post-failover write/read OK"

# Best-effort data presence — replication may not have shipped seed keys
# (ROADMAP: kividb replication RDB bulk-header bug).
PRESENT=0
MISSING=0
for i in $(seq 0 $((SEED_KEYS - 1))); do
  val="$(redis_cli_master "${E2E_KIVIDB_NS}" "${NAME}" GET "e2e:seed:${i}" || true)"
  if [[ "${val}" == "seed-${i}" ]]; then
    PRESENT=$((PRESENT + 1))
  else
    MISSING=$((MISSING + 1))
  fi
done
log "seed key presence after failover: present=${PRESENT} missing=${MISSING}/${SEED_KEYS}"
if (( MISSING > 0 )); then
  warn "some seed keys missing — expected if replica lag / upstream replication bug (see docs/ROADMAP.md 'kividb replication bug'). Not failing the suite solely for this."
fi

# Stop background load if still running.
if [[ -n "${LOAD_PID}" ]] && kill -0 "${LOAD_PID}" 2>/dev/null; then
  kill "${LOAD_PID}" 2>/dev/null || true
  wait "${LOAD_PID}" 2>/dev/null || true
fi

cleanup_cluster "${E2E_KIVIDB_NS}" "${NAME}"
log "failover suite PASS"
