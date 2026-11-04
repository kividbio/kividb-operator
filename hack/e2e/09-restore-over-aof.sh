#!/usr/bin/env bash
# 09-restore-over-aof.sh — bootstrapFromSnapshot over a kept volume whose
# append-only file holds writes the snapshot does not have (issue #58).
#
# A cluster is backed up, takes more writes, and is deleted; its pod-0 PVC
# is kept, as the StatefulSet keeps it. Its log then gets one more write,
# and the volume gets the files engine 1.2.1 and its temp files leave
# (superseded.* logs kept aside, appendonly.aof.tmp, dump.kdb.*.tmp). A new
# cluster with the same name is bootstrapped from the backup onto that
# volume. Its dataset must be the snapshot's exactly, and none of the
# earlier cluster's logs may be left on the volume.
#
# Needs MinIO (03-minio.sh).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

if [[ "${SKIP_RESTORE:-0}" == "1" ]]; then
  log "SKIP_RESTORE=1 — skipping restore-over-AOF suite"
  exit 0
fi
if [[ "${SKIP_MINIO:-0}" == "1" ]]; then
  log "SKIP_MINIO=1 — skipping restore-over-AOF suite (it needs MinIO)"
  exit 0
fi

if [[ -f "${RESULTS_DIR}/minio.env" ]]; then
  # shellcheck source=/dev/null
  source "${RESULTS_DIR}/minio.env"
fi

log "=== restore over a kept volume with an AOF (MinIO ${MINIO_ENDPOINT}) ==="
require kubectl redis-cli

ensure_ns "${E2E_KIVIDB_NS}"
if ! kubectl get svc -n "${E2E_NS}" minio >/dev/null 2>&1; then
  die "MinIO not found in ${E2E_NS} — run 03-minio.sh first (or unset SKIP_MINIO)"
fi

NAME="restore-aof"
IMAGE="$(kividb_image_for_variant standard)"
CRONJOB="${NAME}-backup"
PVC="data-${NAME}-0"
HELPER_IMAGE="${HELPER_IMAGE:-busybox:1.36}"
KEYS=30

cleanup_cluster "${E2E_KIVIDB_NS}" "${NAME}" || true

kubectl apply -n "${E2E_KIVIDB_NS}" -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: ${NAME}-s3
type: Opaque
stringData:
  accessKeyId: "${MINIO_ROOT_USER}"
  secretAccessKey: "${MINIO_ROOT_PASSWORD}"
---
apiVersion: kividb.io/v1alpha1
kind: KividbConfig
metadata:
  name: ${NAME}-config
spec:
  directives:
    maxmemory: "64mb"
---
apiVersion: kividb.io/v1alpha1
kind: KividbSnapshotConfig
metadata:
  name: ${NAME}-snap
spec:
  # Rare schedule: the backup is taken by a Job made from the CronJob.
  schedule: "0 0 1 1 *"
  retention: 5
  source: master
  timeoutSeconds: 120
  s3:
    endpoint: "${MINIO_ENDPOINT}"
    bucket: "${MINIO_BUCKET}"
    region: "us-east-1"
    pathPrefix: "e2e/${NAME}"
    forcePathStyle: true
    insecureSkipTLSVerify: true
    credentialsSecretRef:
      name: ${NAME}-s3
  jobResources:
    requests:
      cpu: 25m
      memory: 32Mi
    limits:
      cpu: 100m
      memory: 64Mi
EOF

# cluster_yaml EXTRA: the cluster, with EXTRA appended to its spec.
cluster_yaml() {
  cat <<EOF
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
  monitoring:
    enabled: false
$1
EOF
}

# --- The earlier cluster: backed up, then more writes ---------------------
cluster_yaml "  snapshotConfigRef:
    name: ${NAME}-snap" | kubectl apply -n "${E2E_KIVIDB_NS}" -f -
wait_cluster_phase "${E2E_KIVIDB_NS}" "${NAME}" "Running" 360
wait_pods_ready "${E2E_KIVIDB_NS}" "kividb.io/cluster=${NAME}" 240

set_args=()
for i in $(seq 1 "${KEYS}"); do set_args+=("k${i}" "v${i}"); done
redis_cli_master "${E2E_KIVIDB_NS}" "${NAME}" MSET "${set_args[@]}" >/dev/null
redis_cli_master "${E2E_KIVIDB_NS}" "${NAME}" INCRBY counter 10 >/dev/null

BACKUP_JOB="${NAME}-seed-$(date +%s)"
log "backing up the earlier cluster with Job ${BACKUP_JOB}"
kubectl create job -n "${E2E_KIVIDB_NS}" "${BACKUP_JOB}" --from="cronjob/${CRONJOB}"
wait_for_condition 240 "backup Job ${BACKUP_JOB} succeeded" \
  bash -c '
    succ=$(kubectl get job -n "$1" "$2" -o jsonpath="{.status.succeeded}" 2>/dev/null || echo 0)
    [[ "${succ:-0}" -ge 1 ]]
  ' bash "${E2E_KIVIDB_NS}" "${BACKUP_JOB}"
wait_snapshot_succeeded "${E2E_KIVIDB_NS}" "${NAME}" 120
SNAP="$(kubectl get kdbs -n "${E2E_KIVIDB_NS}" -l "kividb.io/cluster=${NAME}" \
  -o jsonpath='{range .items[?(@.status.phase=="Succeeded")]}{.metadata.name}{"\n"}{end}' | head -n1)"
[[ -n "${SNAP}" ]] || die "no Succeeded KividbSnapshot for ${NAME}"
log "snapshot ${SNAP}: ${KEYS} keys k1..k${KEYS} and counter=10"

# Writes the snapshot does not have: in the earlier cluster's log.
extra_args=()
for i in $(seq 1 20); do extra_args+=("x${i}" "extra"); done
redis_cli_master "${E2E_KIVIDB_NS}" "${NAME}" MSET "${extra_args[@]}" >/dev/null
redis_cli_master "${E2E_KIVIDB_NS}" "${NAME}" DEL k1 >/dev/null
redis_cli_master "${E2E_KIVIDB_NS}" "${NAME}" INCRBY counter 5 >/dev/null
sleep 2

# --- Delete it; its pod-0 PVC stays ----------------------------------------
log "deleting the earlier cluster; ${PVC} is kept"
kubectl delete kividbcluster -n "${E2E_KIVIDB_NS}" "${NAME}" --wait=true --timeout=180s
wait_for_condition 180 "the earlier cluster's StatefulSet and pods are gone" \
  bash -c '
    ! kubectl get statefulset -n "$1" "$2" >/dev/null 2>&1 || exit 1
    n=$(kubectl get pods -n "$1" -l "app.kubernetes.io/name=kividb,kividb.io/cluster=$2" -o name 2>/dev/null | wc -l | tr -d " ")
    [[ "${n:-0}" -eq 0 ]]
  ' bash "${E2E_KIVIDB_NS}" "${NAME}"
kubectl get pvc -n "${E2E_KIVIDB_NS}" "${PVC}" >/dev/null || die "${PVC} was not kept"

# run_on_volume POD SCRIPT: run SCRIPT in a busybox pod with the PVC at
# /data, print its output, and fail if it fails.
run_on_volume() {
  local pod="$1" script="$2"
  kubectl delete pod -n "${E2E_KIVIDB_NS}" "${pod}" --ignore-not-found --wait=true >/dev/null
  kubectl apply -n "${E2E_KIVIDB_NS}" -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${pod}
  labels:
    app.kubernetes.io/part-of: kividb-e2e
spec:
  restartPolicy: Never
  securityContext:
    runAsUser: 0
  containers:
    - name: sh
      image: ${HELPER_IMAGE}
      imagePullPolicy: IfNotPresent
      command: ["/bin/sh", "-c"]
      args:
        - |
$(printf '%s\n' "${script}" | sed 's/^/          /')
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: ${PVC}
EOF
  wait_for_condition 180 "pod ${pod} finished" \
    bash -c '
      phase=$(kubectl get pod -n "$1" "$2" -o jsonpath="{.status.phase}" 2>/dev/null)
      [[ "${phase}" == "Succeeded" || "${phase}" == "Failed" ]]
    ' bash "${E2E_KIVIDB_NS}" "${pod}"
  kubectl logs -n "${E2E_KIVIDB_NS}" "${pod}"
  local phase
  phase="$(kubectl get pod -n "${E2E_KIVIDB_NS}" "${pod}" -o jsonpath='{.status.phase}')"
  kubectl delete pod -n "${E2E_KIVIDB_NS}" "${pod}" --wait=true >/dev/null
  [[ "${phase}" == "Succeeded" ]] || die "pod ${pod} failed"
}

# One more write in the earlier cluster's log (a RESP command, which the
# engine replays after a delta header or alone), and what engine 1.2.1 and
# the engine's temp files leave: a log kept aside holding a write, and two
# temp files.
log "adding a write to the kept volume's appendonly.aof, and engine leftovers"
run_on_volume "${NAME}-plant" "set -e
cd /data
ls -la
printf '*3\r\n\$3\r\nSET\r\n\$2\r\nz2\r\n\$3\r\nold\r\n' >> appendonly.aof
printf '*3\r\n\$3\r\nSET\r\n\$2\r\nz1\r\n\$3\r\nold\r\n' > superseded.1762000000000.0.appendonly.aof
printf 'x' > appendonly.aof.tmp
printf 'x' > dump.kdb.1.0.tmp
chmod 660 appendonly.aof superseded.1762000000000.0.appendonly.aof appendonly.aof.tmp dump.kdb.1.0.tmp
ls -la"

# --- A new cluster with the name, bootstrapped from the snapshot ----------
log "re-creating ${NAME} with bootstrapFromSnapshot ${SNAP} over ${PVC}"
cluster_yaml "  bootstrapFromSnapshot:
    snapshotRef:
      name: ${SNAP}" | kubectl apply -n "${E2E_KIVIDB_NS}" -f -
wait_for_condition 300 "bootstrap of ${NAME} completed" \
  bash -c '[[ "$(kubectl get kividbcluster -n "$1" "$2" -o jsonpath="{.status.bootstrap.completed}" 2>/dev/null)" == "true" ]]' \
  bash "${E2E_KIVIDB_NS}" "${NAME}"
kubectl get kividbcluster -n "${E2E_KIVIDB_NS}" "${NAME}" -o jsonpath='{.status.bootstrap.message}{"\n"}'
has_event_reason "${E2E_KIVIDB_NS}" KividbCluster "${NAME}" BootstrapOverExistingVolume \
  || die "no BootstrapOverExistingVolume event: the restore did not go over the kept volume"
log "restore Job log:"
kubectl logs -n "${E2E_KIVIDB_NS}" "job/${NAME}-bootstrap" --tail=40 || true
wait_cluster_phase "${E2E_KIVIDB_NS}" "${NAME}" "Running" 360
wait_pods_ready "${E2E_KIVIDB_NS}" "kividb.io/cluster=${NAME}" 240

# --- The dataset is the snapshot's, exactly --------------------------------
size="$(redis_cli_master "${E2E_KIVIDB_NS}" "${NAME}" DBSIZE | tr -d '[:space:]')"
get_args=()
for i in $(seq 1 "${KEYS}"); do get_args+=("k${i}"); done
got="$(redis_cli_master "${E2E_KIVIDB_NS}" "${NAME}" MGET "${get_args[@]}" counter | tr '\n' ' ')"
want="$(for i in $(seq 1 "${KEYS}"); do printf 'v%s ' "${i}"; done)10 "
gone_args=(z1 z2)
for i in $(seq 1 20); do gone_args+=("x${i}"); done
extra="$(redis_cli_master "${E2E_KIVIDB_NS}" "${NAME}" EXISTS "${gone_args[@]}" | tr -d '[:space:]')"
log "restored: DBSIZE=${size}, keys from the earlier cluster's log present: ${extra}"
[[ "${size}" == "$((KEYS + 1))" ]] || die "DBSIZE is ${size}, want $((KEYS + 1)): the snapshot's keys only"
[[ "${got}" == "${want}" ]] || die "values differ from the snapshot: got '${got}', want '${want}'"
[[ "${extra}" == "0" ]] || die "${extra} key(s) from the earlier cluster's log are in the restored data"

# --- None of the earlier cluster's logs are left -------------------------
log "files on ${PVC} after the restore:"
run_on_volume "${NAME}-check" "cd /data
ls -la
left=\$(ls -1A | grep -E '^superseded\\.|^appendonly\\.aof\\.tmp\$|^dump\\.kdb\\.1\\.0\\.tmp\$' || true)
if [ -n \"\$left\" ]; then echo \"left from the earlier cluster: \$left\"; exit 1; fi"

cleanup_cluster "${E2E_KIVIDB_NS}" "${NAME}"
kubectl delete job -n "${E2E_KIVIDB_NS}" -l "app.kubernetes.io/name=kividb-backup" --ignore-not-found || true
log "restore-over-AOF suite PASS"
