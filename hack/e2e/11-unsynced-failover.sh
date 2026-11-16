#!/usr/bin/env bash
# 11-unsynced-failover.sh — the master's node dies while no replica is in
# sync with it: the failover must wait for the master, not promote the
# replica (#75).
#
# The one replica of a replicas: 1 cluster in the middle of a full resync
# holds the snapshot it was sent but not the writes queued after it. If the
# master's node dies then, those writes are only on the master's volume.
# Promoting the replica, and resyncing the master from it when it came
# back, lost them: 42,170 acknowledged writes on kind with 7M keys. A full
# resync that long does not fit here, so the replica is put out of sync in
# another way the operator sees alike: its link to the master is cut (port
# 6380 rejected on the node) while the writes go on. Then the master's
# engine and agent are killed with SIGKILL from the node, as a node failure
# would, and its pod is force-deleted. restartWaitSeconds is 0: without the
# wait, the replica would be promoted at once. The suite checks that it was
# not (FailoverBlocked), that the master came back as the master, and that
# no acknowledged write is lost. kind only: it runs crictl and iptables on
# the node.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

if [[ "${SKIP_UNSYNCED:-0}" == "1" ]]; then
  log "SKIP_UNSYNCED=1 — skipping the unsynced failover suite"
  exit 0
fi
if [[ "${CLUSTER_DRIVER}" != "kind" ]]; then
  warn "CLUSTER_DRIVER=${CLUSTER_DRIVER}: the unsynced failover suite needs kind (crictl and iptables on the node); skipping"
  exit 0
fi

log "=== node failure with no replica in sync: the failover waits for the master ==="
require kubectl
require docker

NAME="unsynced"
NS="${E2E_KIVIDB_NS}"
IMAGE="$(kividb_image_for_variant standard)"
WRITER_IMAGE="${E2E_WRITER_IMAGE:-python:3.12-alpine}"

ensure_ns "${NS}"
cleanup_cluster "${NS}" "${NAME}" || true
kubectl delete pod -n "${NS}" unsynced-writer --ignore-not-found --wait=true >/dev/null

kubectl apply -n "${NS}" -f - <<EOF
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
  failover:
    restartWaitSeconds: 0
$(cluster_resources_yaml)
  monitoring:
    enabled: false
EOF
kubectl create configmap fence-writer -n "${NS}" --from-file=writer.py="${SCRIPT_DIR}/fence-writer.py" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

wait_cluster_phase "${NS}" "${NAME}" "Running" 360

# both_synced: both pods Ready and in sync, as the status records them.
both_synced() {
  local rows
  rows="$(kubectl get kividbcluster -n "${NS}" "${NAME}" \
    -o jsonpath='{range .status.pods[*]}{.ready}{.synced}{"\n"}{end}' 2>/dev/null)"
  [[ "$(grep -c . <<<"${rows}")" == 2 && "$(grep -c '^truetrue$' <<<"${rows}")" == 2 ]]
}
wait_for_condition 300 "both pods of ${NAME} Ready and in sync" both_synced

master="$(master_pod "${NS}" "${NAME}")"
replica="$(kubectl get pods -n "${NS}" -l "kividb.io/cluster=${NAME},kividb.io/role=replica" -o jsonpath='{.items[0].metadata.name}')"
[[ -n "${master}" && -n "${replica}" ]] || die "no master or replica pod (master=${master}, replica=${replica})"
master_ip="$(kubectl get pod -n "${NS}" "${master}" -o jsonpath='{.status.podIP}')"
replica_ip="$(kubectl get pod -n "${NS}" "${replica}" -o jsonpath='{.status.podIP}')"
node="$(kubectl get pod -n "${NS}" "${master}" -o jsonpath='{.spec.nodeName}')"

rule=(FORWARD -s "${replica_ip}" -d "${master_ip}" -p tcp --dport "${KIVIDB_PORT}" -j REJECT --reject-with tcp-reset)
unblock() { docker exec "${node}" iptables -D "${rule[@]}" 2>/dev/null || true; }
trap unblock EXIT

kubectl apply -n "${NS}" -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: unsynced-writer
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
    - {name: PREFIX, value: "u-$(date +%s)"}
    - {name: DURATION, value: "90"}
    - {name: SETTLE, value: "30"}
    volumeMounts: [{name: writer, mountPath: /writer}]
  volumes:
  - name: writer
    configMap: {name: fence-writer}
EOF
kubectl wait -n "${NS}" --for=condition=Ready pod/unsynced-writer --timeout=180s >/dev/null
sleep 5

docker exec "${node}" iptables -I "${rule[@]}"
log "cut ${replica}'s link to ${master} (${replica_ip} -> ${master_ip}:${KIVIDB_PORT} rejected)"
# synced is left out of the status when false.
replica_out_of_sync() {
  [[ "$(kubectl get kividbcluster -n "${NS}" "${NAME}" \
    -o jsonpath="{.status.pods[?(@.name=='${replica}')].synced}" 2>/dev/null)" != "true" ]]
}
wait_for_condition 90 "${replica} out of sync in the status" replica_out_of_sync
sleep 3 # writes the replica does not get

# Both at once, as a node failure would: one after the other, the kubelet
# restarts the engine in place in between.
pids=()
for c in kividb agent; do
  cid="$(docker exec "${node}" crictl ps -q --name "^${c}\$" \
    --label "io.kubernetes.pod.name=${master}" --label "io.kubernetes.pod.namespace=${NS}")"
  [[ -n "${cid}" ]] || die "no ${c} container of ${master} on ${node}"
  pids+=("$(docker exec "${node}" crictl inspect --output go-template --template '{{.info.pid}}' "${cid}")")
done
docker exec "${node}" kill -9 "${pids[@]}"
log "killed ${master}'s engine and agent with SIGKILL, as a node failure would"
kill_pod "${NS}" "${master}"
unblock

kubectl wait -n "${NS}" --for=jsonpath='{.status.phase}'=Succeeded pod/unsynced-writer --timeout=300s >/dev/null \
  || die "the writer did not finish: $(kubectl logs -n "${NS}" unsynced-writer --tail=5 2>&1)"
result="$(kubectl logs -n "${NS}" unsynced-writer | tail -1)"
acked="$(sed -n 's/.*"acked": \([0-9]*\).*/\1/p' <<<"${result}")"
lost="$(sed -n 's/.*"lost": \([0-9]*\).*/\1/p' <<<"${result}")"
after="$(sed -n 's/.*"acked_after_error": \([0-9]*\).*/\1/p' <<<"${result}")"
[[ -n "${acked}" && -n "${lost}" && -n "${after}" ]] || die "unreadable writer result: ${result}"
log "${acked} writes acknowledged (${after} after ${master} went away), ${lost} lost"

# Read the events whole first: piped into grep -q, kubectl is killed by
# SIGPIPE once grep has its match, and pipefail turns a match into a miss.
blocked="$(kubectl get events -n "${NS}" \
  --field-selector "involvedObject.kind=KividbCluster,involvedObject.name=${NAME},reason=FailoverBlocked" \
  -o jsonpath='{range .items[*]}{.message}{"\n"}{end}' 2>/dev/null || true)"
now_master="$(master_pod "${NS}" "${NAME}")"

kubectl delete pod -n "${NS}" unsynced-writer --ignore-not-found >/dev/null
kubectl delete configmap -n "${NS}" fence-writer --ignore-not-found >/dev/null

(( lost == 0 )) || die "${lost} of ${acked} acknowledged writes lost (${result})"
(( acked > 0 )) || die "the writer could not write at all (${result})"
(( after > 0 )) || die "no write went through after ${master} went away (${result})"
grep -q "no replica was in sync" <<<"${blocked}" \
  || die "no FailoverBlocked event saying no replica was in sync: the failover did not wait for ${master}"
[[ "${now_master}" == "${master}" ]] || die "the master is ${now_master:-none}, want ${master} kept once it came back"

cleanup_cluster "${NS}" "${NAME}"
log "unsynced failover suite PASS"
