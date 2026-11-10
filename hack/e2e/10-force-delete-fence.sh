#!/usr/bin/env bash
# 10-force-delete-fence.sh — force-delete the master under write load and
# count the acknowledged writes the failover loses (#53).
#
# A force-deleted master's pod object is gone at once, but its container
# runs on until its replicas have left it, acknowledging the writes that
# come in on connections clients already had to it. Before promoting a
# replica the operator holds those writes (CLIENT PAUSE WRITE through the
# old master's agent); without that, every write it acknowledged after the
# promotion was lost -- 689 to 954 per force-delete on kind.
#
# Each attempt waits until every pod is Ready and in sync (a failover while
# a replica is still resyncing loses writes for another reason, #75), runs
# fence-writer.py in a pod against the master Service, force-deletes the
# master, and fails on any acknowledged write that is missing. A
# force-delete does not always fail over -- the operator may still see the
# pod being deleted and switch over, or the engine restarts in place first
# -- so attempts repeat until one went through the fenced failover,
# FENCE_ATTEMPTS at most.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

if [[ "${SKIP_FENCE:-0}" == "1" ]]; then
  log "SKIP_FENCE=1 — skipping the force-delete fence suite"
  exit 0
fi

log "=== force-delete under load: acknowledged writes lost ==="
require kubectl

ensure_ns "${E2E_KIVIDB_NS}"

NAME="fence-load"
IMAGE="$(kividb_image_for_variant standard)"
WRITER_IMAGE="${E2E_WRITER_IMAGE:-python:3.12-alpine}"
ATTEMPTS="${FENCE_ATTEMPTS:-8}"
NS="${E2E_KIVIDB_NS}"

cleanup_cluster "${NS}" "${NAME}" || true
kubectl delete pod -n "${NS}" fence-writer --ignore-not-found --wait=true >/dev/null

kubectl apply -n "${NS}" -f - <<EOF
apiVersion: kividb.io/v1alpha1
kind: KividbCluster
metadata:
  name: ${NAME}
  labels:
    app.kubernetes.io/part-of: kividb-e2e
spec:
  replicas: 2
  image: ${IMAGE}
  imagePullPolicy: IfNotPresent
  variant: standard
  agentImage: ${AGENT_IMG}
  port: ${KIVIDB_PORT}
$(cluster_resources_yaml)
  monitoring:
    enabled: false
EOF
kubectl create configmap fence-writer -n "${NS}" --from-file=writer.py="${SCRIPT_DIR}/fence-writer.py" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

wait_cluster_phase "${NS}" "${NAME}" "Running" 360

# all_synced: every one of the three pods Ready and in sync, as the status
# records them.
all_synced() {
  local rows
  rows="$(kubectl get kividbcluster -n "${NS}" "${NAME}" \
    -o jsonpath='{range .status.pods[*]}{.ready}{.synced}{"\n"}{end}' 2>/dev/null)"
  [[ "$(grep -c . <<<"${rows}")" == 3 && "$(grep -c '^truetrue$' <<<"${rows}")" == 3 ]]
}

fenced=0
for attempt in $(seq 1 "${ATTEMPTS}"); do
  wait_for_condition 300 "every pod of ${NAME} Ready and in sync" all_synced
  master="$(master_pod "${NS}" "${NAME}")"
  [[ -n "${master}" ]] || die "no master pod"

  kubectl delete pod -n "${NS}" fence-writer --ignore-not-found --wait=true >/dev/null
  kubectl apply -n "${NS}" -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: fence-writer
  labels:
    app.kubernetes.io/part-of: kividb-e2e
spec:
  restartPolicy: Never
  containers:
  - name: writer
    image: ${WRITER_IMAGE}
    command: [python, -u, /writer/writer.py]
    env:
    - {name: HOST, value: "${NAME}-master"}
    - {name: PORT, value: "${KIVIDB_PORT}"}
    - {name: PREFIX, value: "a${attempt}-$(date +%s)"}
    - {name: DURATION, value: "30"}
    - {name: SETTLE, value: "20"}
    volumeMounts: [{name: writer, mountPath: /writer}]
  volumes:
  - name: writer
    configMap: {name: fence-writer}
EOF
  kubectl wait -n "${NS}" --for=condition=Ready pod/fence-writer --timeout=180s >/dev/null
  sleep 8

  since="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  kill_pod "${NS}" "${master}"
  kubectl wait -n "${NS}" --for=jsonpath='{.status.phase}'=Succeeded pod/fence-writer --timeout=240s >/dev/null \
    || die "attempt ${attempt}: the writer did not finish: $(kubectl logs -n "${NS}" fence-writer --tail=5 2>&1)"
  result="$(kubectl logs -n "${NS}" fence-writer | tail -1)"
  acked="$(sed -n 's/.*"acked": \([0-9]*\).*/\1/p' <<<"${result}")"
  lost="$(sed -n 's/.*"lost": \([0-9]*\).*/\1/p' <<<"${result}")"
  [[ -n "${acked}" && -n "${lost}" ]] || die "attempt ${attempt}: unreadable writer result: ${result}"
  log "attempt ${attempt}: force-deleted ${master}; ${acked} writes acknowledged, ${lost} lost"
  (( lost == 0 )) || die "attempt ${attempt}: ${lost} of ${acked} acknowledged writes lost after force-deleting ${master} (${result})"

  if kubectl logs -n "${OPERATOR_NS}" deploy/kividb-operator-manager --since-time="${since}" 2>/dev/null \
      | grep -q "fenced the master being failed away from"; then
    fenced=$((fenced + 1))
    log "attempt ${attempt}: the operator held ${master}'s writes before failing over"
    break
  fi
  log "attempt ${attempt}: no fenced failover this time (a switchover, or the engine restarted in place); again"
done

kubectl delete pod -n "${NS}" fence-writer --ignore-not-found >/dev/null
kubectl delete configmap -n "${NS}" fence-writer --ignore-not-found >/dev/null
(( fenced > 0 )) || die "no attempt in ${ATTEMPTS} went through a fenced failover: the fence was not exercised"

cleanup_cluster "${NS}" "${NAME}"
log "force-delete fence suite PASS"
