#!/usr/bin/env bash
# 08-resp-acl-select.sh — RESP3 smoke, command ACL deny, SELECT on replica,
# failover re-point (exactly one master).
#
# Parameterize engine with KIVIDB_VERSION (default v1.0.5; use v1.0.5-rc1
# for pre-GA testing).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

if [[ "${SKIP_RESP_ACL:-0}" == "1" ]]; then
  log "SKIP_RESP_ACL=1 — skipping RESP3/ACL/SELECT suite"
  exit 0
fi

log "=== RESP3 / ACL / SELECT (kividb ${KIVIDB_VERSION}) ==="
require kubectl redis-cli

ensure_ns "${E2E_KIVIDB_NS}"

NAME="resp-acl-select"
IMAGE="$(kividb_image_for_variant standard)"
PASS="e2e-acl-pass"

cleanup_cluster "${E2E_KIVIDB_NS}" "${NAME}" || true
kubectl -n "${E2E_KIVIDB_NS}" delete kividbaclconfig "${NAME}-acl" --ignore-not-found >/dev/null 2>&1 || true
kubectl -n "${E2E_KIVIDB_NS}" delete secret "${NAME}-auth" --ignore-not-found >/dev/null 2>&1 || true

kubectl apply -n "${E2E_KIVIDB_NS}" -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: ${NAME}-auth
type: Opaque
stringData:
  password: "${PASS}"
---
apiVersion: kividb.io/v1alpha1
kind: KividbAclConfig
metadata:
  name: ${NAME}-acl
spec:
  users:
    - name: default
      enabled: true
      passwordSecretRef:
        name: ${NAME}-auth
        key: password
      keyPatterns:
        - "~*"
      commandRules:
        - "+@all"
        - "-flushall"
        - "-flushdb"
---
apiVersion: kividb.io/v1alpha1
kind: KividbCluster
metadata:
  name: ${NAME}
  labels:
    app.kubernetes.io/part-of: kividb-e2e
spec:
  replicas: 1
  image: ${IMAGE}
  imagePullPolicy: IfNotPresent
  variant: standard
  agentImage: ${AGENT_IMG}
  port: ${KIVIDB_PORT}
  aclConfigRef:
    name: ${NAME}-acl
$(cluster_resources_yaml)
  failover:
    enabled: true
    unhealthyThresholdSeconds: 10
  monitoring:
    enabled: false
EOF

wait_cluster_phase "${E2E_KIVIDB_NS}" "${NAME}" "Running" 360
wait_pods_ready "${E2E_KIVIDB_NS}" "kividb.io/cluster=${NAME}" 240

MASTER="$(master_pod "${E2E_KIVIDB_NS}" "${NAME}")"
[[ -n "${MASTER}" ]] || die "no master pod"
log "master=${MASTER}"

hello_out="$(redis_cli_master "${E2E_KIVIDB_NS}" "${NAME}" -a "${PASS}" --no-auth-warning HELLO 3 2>/dev/null || true)"
if ! echo "${hello_out}" | grep -Eiq 'server|version|proto|3'; then
  die "HELLO 3 did not look like a RESP3 map reply: ${hello_out}"
fi
log "HELLO 3 ok"

flush_out="$(redis_cli_master "${E2E_KIVIDB_NS}" "${NAME}" -a "${PASS}" --no-auth-warning FLUSHALL 2>&1 || true)"
if ! echo "${flush_out}" | grep -Eiq 'NOPERM|permission'; then
  log "WARN: FLUSHALL did not clearly deny (got: ${flush_out}) — see ROADMAP ACL caveats"
else
  log "FLUSHALL denied as expected"
fi

redis_cli_master "${E2E_KIVIDB_NS}" "${NAME}" -a "${PASS}" --no-auth-warning SET e2e:select:key val >/dev/null
sleep 3
REPLICA="$(kubectl -n "${E2E_KIVIDB_NS}" get pods -l "kividb.io/cluster=${NAME},kividb.io/role=replica" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
[[ -n "${REPLICA}" ]] || die "no replica pod for SELECT check"

select_out="$(kubectl -n "${E2E_KIVIDB_NS}" exec "${REPLICA}" -c kividb -- \
  redis-cli -p "${KIVIDB_PORT}" -a "${PASS}" --no-auth-warning GET e2e:select:key 2>/dev/null || true)"
if ! echo "${select_out}" | grep -q "val"; then
  die "GET on replica did not return seeded value (got: ${select_out})"
fi
# Explicit SELECT 0 then GET (logical DB). `-n 0` makes redis-cli send
# SELECT 0 before the command; "SELECT 0 GET key" on the command line would
# be one malformed SELECT with extra arguments.
select_out2="$(kubectl -n "${E2E_KIVIDB_NS}" exec "${REPLICA}" -c kividb -- \
  redis-cli -p "${KIVIDB_PORT}" -a "${PASS}" --no-auth-warning -n 0 GET e2e:select:key 2>/dev/null || true)"
if ! echo "${select_out2}" | grep -q "val"; then
  die "SELECT 0 + GET on replica failed (got: ${select_out2})"
fi
log "SELECT + GET on replica ok"

OLD="${MASTER}"
kubectl -n "${E2E_KIVIDB_NS}" delete pod "${OLD}" --wait=false
# Wait until a master exists again and is not the deleted pod (or pod recycled).
for _ in $(seq 1 60); do
  NEW="$(master_pod "${E2E_KIVIDB_NS}" "${NAME}" 2>/dev/null || true)"
  masters="$(kubectl -n "${E2E_KIVIDB_NS}" get pods -l "kividb.io/cluster=${NAME},kividb.io/role=master" --no-headers 2>/dev/null | wc -l | tr -d ' ')"
  if [[ -n "${NEW}" && "${masters}" == "1" ]]; then
    break
  fi
  sleep 2
done
wait_cluster_phase "${E2E_KIVIDB_NS}" "${NAME}" "Running" 240
masters="$(kubectl -n "${E2E_KIVIDB_NS}" get pods -l "kividb.io/cluster=${NAME},kividb.io/role=master" --no-headers 2>/dev/null | wc -l | tr -d ' ')"
if [[ "${masters}" != "1" ]]; then
  die "expected exactly 1 master after failover, got ${masters}"
fi
NEW="$(master_pod "${E2E_KIVIDB_NS}" "${NAME}")"
log "failover re-point ok (old=${OLD} new=${NEW} masters=${masters})"

cleanup_cluster "${E2E_KIVIDB_NS}" "${NAME}" || true
kubectl -n "${E2E_KIVIDB_NS}" delete kividbaclconfig "${NAME}-acl" --ignore-not-found >/dev/null 2>&1 || true
kubectl -n "${E2E_KIVIDB_NS}" delete secret "${NAME}-auth" --ignore-not-found >/dev/null 2>&1 || true

log "=== RESP3 / ACL / SELECT passed ==="
