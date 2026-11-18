#!/usr/bin/env bash
# 12-dir-under-data.sh — a KividbConfig `dir` under /data (issue #112).
#
# The engine changes into `dir` at start and does not create it. A new
# cluster with `dir: sub` and a replica must start: the data-dir init
# container makes /data/sub on each pod's volume, and the engine keeps its
# files there. With MinIO, a backup of it must hold what it wrote (the
# agent packs /data/sub, not /data, which holds nothing), and a cluster
# bootstrapped from that backup with the same `dir` must start with the
# data (the restore extracts into /data/sub, which it makes). Last, the
# first cluster's kept volume: its files are in /data/sub, none in /data,
# and /data/sub has the mode of /data.
#
# Without MinIO (SKIP_MINIO=1) the backup and the bootstrap are skipped.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

if [[ "${SKIP_DIR:-0}" == "1" ]]; then
  log "SKIP_DIR=1 — skipping dir-under-data suite"
  exit 0
fi
WITH_MINIO=1
if [[ "${SKIP_MINIO:-0}" == "1" ]]; then
  WITH_MINIO=0
fi

if [[ -f "${RESULTS_DIR}/minio.env" ]]; then
  # shellcheck source=/dev/null
  source "${RESULTS_DIR}/minio.env"
fi

log "=== KividbConfig dir under /data (backup and bootstrap: $([[ ${WITH_MINIO} == 1 ]] && echo yes || echo "no, SKIP_MINIO=1")) ==="
require kubectl redis-cli

ensure_ns "${E2E_KIVIDB_NS}"
if [[ "${WITH_MINIO}" == "1" ]] && ! kubectl get svc -n "${E2E_NS}" minio >/dev/null 2>&1; then
  die "MinIO not found in ${E2E_NS} — run 03-minio.sh first (or set SKIP_MINIO=1)"
fi

NAME="dir-sub"
RESTORED="dir-sub-restored"
IMAGE="$(kividb_image_for_variant standard)"
CRONJOB="${NAME}-backup"
PVC="data-${NAME}-0"
HELPER_IMAGE="${HELPER_IMAGE:-busybox:1.36}"
KEYS=30

cleanup_cluster "${E2E_KIVIDB_NS}" "${RESTORED}" || true
cleanup_cluster "${E2E_KIVIDB_NS}" "${NAME}" || true

kubectl apply -n "${E2E_KIVIDB_NS}" -f - <<EOF
apiVersion: kividb.io/v1alpha1
kind: KividbConfig
metadata:
  name: ${NAME}-config
spec:
  directives:
    dir: "sub"
    maxmemory: "64mb"
EOF
if [[ "${WITH_MINIO}" == "1" ]]; then
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
fi

# cluster_yaml NAME REPLICAS EXTRA: a cluster with the `dir: sub`
# KividbConfig, with EXTRA appended to its spec.
cluster_yaml() {
  cat <<EOF
apiVersion: kividb.io/v1alpha1
kind: KividbCluster
metadata:
  name: $1
  labels:
    app.kubernetes.io/part-of: kividb-e2e
spec:
  replicas: $2
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
$3
EOF
}

# init_log POD: what the pod's data-dir init container logged.
init_log() {
  kubectl logs -n "${E2E_KIVIDB_NS}" "$1" -c data-dir 2>&1 || true
}

# engine_dir CLUSTER: the directory the cluster's master keeps its files in.
engine_dir() {
  redis_cli_master "${E2E_KIVIDB_NS}" "$1" CONFIG GET dir | tail -n1 | tr -d '\r'
}

# --- A new cluster with dir: sub starts --------------------------------------
extra=""
if [[ "${WITH_MINIO}" == "1" ]]; then
  extra="  snapshotConfigRef:
    name: ${NAME}-snap"
fi
cluster_yaml "${NAME}" 1 "${extra}" | kubectl apply -n "${E2E_KIVIDB_NS}" -f -
wait_cluster_phase "${E2E_KIVIDB_NS}" "${NAME}" "Running" 360
wait_pods_ready "${E2E_KIVIDB_NS}" "kividb.io/cluster=${NAME}" 240

for pod in "${NAME}-0" "${NAME}-1"; do
  out="$(init_log "${pod}")"
  log "${pod} data-dir: ${out}"
  grep -q "made /data/sub" <<<"${out}" || die "${pod}: the init container did not make /data/sub"
done
dir="$(engine_dir "${NAME}")"
[[ "${dir}" == "/data/sub" ]] || die "the master keeps its files in '${dir}', want /data/sub"

set_args=()
for i in $(seq 1 "${KEYS}"); do set_args+=("k${i}" "v${i}"); done
redis_cli_master "${E2E_KIVIDB_NS}" "${NAME}" MSET "${set_args[@]}" >/dev/null
redis_cli_master "${E2E_KIVIDB_NS}" "${NAME}" INCRBY counter 10 >/dev/null
log "${NAME}: started with dir /data/sub on both pods, ${KEYS} keys k1..k${KEYS} and counter=10 written"

# --- A backup of it, and a cluster bootstrapped from that backup -------------
if [[ "${WITH_MINIO}" == "1" ]]; then
  BACKUP_JOB="${NAME}-seed-$(date +%s)"
  log "backing up ${NAME} with Job ${BACKUP_JOB}"
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
  log "snapshot ${SNAP}"

  log "creating ${RESTORED} with bootstrapFromSnapshot ${SNAP} and the same dir"
  cluster_yaml "${RESTORED}" 0 "  bootstrapFromSnapshot:
    snapshotRef:
      name: ${SNAP}" | kubectl apply -n "${E2E_KIVIDB_NS}" -f -
  wait_for_condition 300 "bootstrap of ${RESTORED} completed" \
    bash -c '[[ "$(kubectl get kividbcluster -n "$1" "$2" -o jsonpath="{.status.bootstrap.completed}" 2>/dev/null)" == "true" ]]' \
    bash "${E2E_KIVIDB_NS}" "${RESTORED}"
  log "restore Job log:"
  restore_log="$(kubectl logs -n "${E2E_KIVIDB_NS}" "job/${RESTORED}-bootstrap" --tail=40 2>&1 || true)"
  printf '%s\n' "${restore_log}"
  grep -q "made /data/sub" <<<"${restore_log}" || die "the restore did not make /data/sub"
  grep -q "restored snapshot into /data/sub" <<<"${restore_log}" || die "the restore did not extract into /data/sub"
  wait_cluster_phase "${E2E_KIVIDB_NS}" "${RESTORED}" "Running" 360
  wait_pods_ready "${E2E_KIVIDB_NS}" "kividb.io/cluster=${RESTORED}" 240

  out="$(init_log "${RESTORED}-0")"
  log "${RESTORED}-0 data-dir: ${out}"
  grep -q "/data/sub is there already" <<<"${out}" || die "${RESTORED}-0: the init container did not find the restore's /data/sub"
  dir="$(engine_dir "${RESTORED}")"
  [[ "${dir}" == "/data/sub" ]] || die "${RESTORED} keeps its files in '${dir}', want /data/sub"

  size="$(redis_cli_master "${E2E_KIVIDB_NS}" "${RESTORED}" DBSIZE | tr -d '[:space:]')"
  get_args=()
  for i in $(seq 1 "${KEYS}"); do get_args+=("k${i}"); done
  got="$(redis_cli_master "${E2E_KIVIDB_NS}" "${RESTORED}" MGET "${get_args[@]}" counter | tr '\n' ' ')"
  want="$(for i in $(seq 1 "${KEYS}"); do printf 'v%s ' "${i}"; done)10 "
  log "${RESTORED}: DBSIZE=${size}"
  [[ "${size}" == "$((KEYS + 1))" ]] || die "DBSIZE is ${size}, want $((KEYS + 1)): the backup's keys"
  [[ "${got}" == "${want}" ]] || die "values differ from what was backed up: got '${got}', want '${want}'"
  cleanup_cluster "${E2E_KIVIDB_NS}" "${RESTORED}"
fi

# --- The first cluster's kept volume -----------------------------------------
log "deleting ${NAME}; ${PVC} is kept"
kubectl delete kividbcluster -n "${E2E_KIVIDB_NS}" "${NAME}" --wait=true --timeout=180s
wait_for_condition 180 "${NAME}'s pods are gone" \
  bash -c '
    n=$(kubectl get pods -n "$1" -l "app.kubernetes.io/name=kividb,kividb.io/cluster=$2" -o name 2>/dev/null | wc -l | tr -d " ")
    [[ "${n:-0}" -eq 0 ]]
  ' bash "${E2E_KIVIDB_NS}" "${NAME}"

CHECK="${NAME}-check"
kubectl delete pod -n "${E2E_KIVIDB_NS}" "${CHECK}" --ignore-not-found --wait=true >/dev/null
kubectl apply -n "${E2E_KIVIDB_NS}" -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${CHECK}
  labels:
    app.kubernetes.io/part-of: kividb-e2e
spec:
  restartPolicy: Never
  containers:
    - name: sh
      image: ${HELPER_IMAGE}
      imagePullPolicy: IfNotPresent
      command: ["/bin/sh", "-c"]
      args:
        - |
          ls -la /data /data/sub
          echo "/data: \$(stat -c '%a %U:%G' /data)  /data/sub: \$(stat -c '%a %U:%G' /data/sub)"
          [ "\$(stat -c %a /data/sub)" = "\$(stat -c %a /data)" ] || { echo "/data/sub has another mode than /data"; exit 1; }
          ls /data/sub | grep -qE '^(dump\.kdb|appendonly\.aof)\$' || { echo "no engine files in /data/sub"; exit 1; }
          if ls /data | grep -qE '^(dump\.kdb|appendonly\.aof)\$'; then echo "engine files in /data"; exit 1; fi
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: ${PVC}
EOF
wait_for_condition 180 "pod ${CHECK} finished" \
  bash -c '
    phase=$(kubectl get pod -n "$1" "$2" -o jsonpath="{.status.phase}" 2>/dev/null)
    [[ "${phase}" == "Succeeded" || "${phase}" == "Failed" ]]
  ' bash "${E2E_KIVIDB_NS}" "${CHECK}"
kubectl logs -n "${E2E_KIVIDB_NS}" "${CHECK}"
phase="$(kubectl get pod -n "${E2E_KIVIDB_NS}" "${CHECK}" -o jsonpath='{.status.phase}')"
kubectl delete pod -n "${E2E_KIVIDB_NS}" "${CHECK}" --wait=true >/dev/null
[[ "${phase}" == "Succeeded" ]] || die "${PVC}: the engine's files are not where they should be"

cleanup_cluster "${E2E_KIVIDB_NS}" "${NAME}"
if [[ -n "${BACKUP_JOB:-}" ]]; then
  kubectl delete job -n "${E2E_KIVIDB_NS}" "${BACKUP_JOB}" --ignore-not-found || true
fi
log "dir-under-data suite PASS"
