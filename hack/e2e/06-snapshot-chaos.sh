#!/usr/bin/env bash
# 06-snapshot-chaos.sh — MinIO-backed snapshots + kill source / Job mid-backup.
#
# StackGres-style: KividbSnapshotConfig is a separate CR (schedule + S3
# destination). The operator creates a CronJob whose backup-trigger container
# POSTs to the agent /backup on the master Service; each run yields a
# KividbSnapshot status object.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

if [[ "${SKIP_SNAPSHOT:-0}" == "1" ]]; then
  log "SKIP_SNAPSHOT=1 — skipping snapshot chaos suite"
  exit 0
fi

if [[ -f "${RESULTS_DIR}/minio.env" ]]; then
  # shellcheck source=/dev/null
  source "${RESULTS_DIR}/minio.env"
fi

log "=== snapshot chaos (MinIO ${MINIO_ENDPOINT}) ==="
require kubectl redis-cli

ensure_ns "${E2E_KIVIDB_NS}"

# Ensure MinIO is up (suite can be run standalone after 03-minio.sh).
if ! kubectl get svc -n "${E2E_NS}" minio >/dev/null 2>&1; then
  die "MinIO not found in ${E2E_NS} — run 03-minio.sh first (or unset SKIP_MINIO)"
fi

NAME="snap-chaos"
IMAGE="$(kividb_image_for_variant standard)"
CRONJOB="${NAME}-backup"

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
  # Rare schedule so the CronJob exists for `kubectl create job --from=...`
  # without racing readiness probes. All suite triggers are manual Jobs.
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
  configRef:
    name: ${NAME}-config
  snapshotConfigRef:
    name: ${NAME}-snap
$(cluster_resources_yaml)
  failover:
    enabled: true
    unhealthyThresholdSeconds: 15
  monitoring:
    enabled: false
EOF

wait_cluster_phase "${E2E_KIVIDB_NS}" "${NAME}" "Running" 360
wait_pods_ready "${E2E_KIVIDB_NS}" "kividb.io/cluster=${NAME}" 240

# Write some data so BGSAVE has content.
for i in $(seq 1 30); do
  redis_cli_master "${E2E_KIVIDB_NS}" "${NAME}" SET "e2e:snap:${i}" "v${i}" >/dev/null
done

# Seed one successful snapshot via a manual Job (schedule is intentionally rare).
SEED_JOB="${NAME}-seed-$(date +%s)"
log "seeding first successful KividbSnapshot via Job ${SEED_JOB}"
kubectl create job -n "${E2E_KIVIDB_NS}" "${SEED_JOB}" --from="cronjob/${CRONJOB}"
wait_for_condition 240 "seed Job ${SEED_JOB} succeeded" \
  bash -c '
    ns="$1"; job="$2"
    succ=$(kubectl get job -n "$ns" "$job" -o jsonpath="{.status.succeeded}" 2>/dev/null || echo 0)
    [[ "${succ:-0}" -ge 1 ]]
  ' bash "${E2E_KIVIDB_NS}" "${SEED_JOB}"
wait_snapshot_succeeded "${E2E_KIVIDB_NS}" "${NAME}" 120

log "first successful snapshot(s):"
kubectl get kdbs -n "${E2E_KIVIDB_NS}" -l "kividb.io/cluster=${NAME}" -o wide || true
kubectl get kdbs -n "${E2E_KIVIDB_NS}" -l "kividb.io/cluster=${NAME}" \
  -o custom-columns=NAME:.metadata.name,PHASE:.status.phase,POD:.status.sourcePod,KEY:.status.objectKey,SIZE:.status.sizeBytes,ERR:.status.error \
  || true

# --- Chaos A: create a Job from the CronJob and kill the SOURCE db pod mid-run ---
log "chaos A: create Job from CronJob + kill source db pod"
JOB_A="${NAME}-chaos-src-$(date +%s)"
kubectl create job -n "${E2E_KIVIDB_NS}" "${JOB_A}" --from="cronjob/${CRONJOB}"

# Wait briefly for the Job pod / backup-trigger to start, then kill master.
sleep 5
SRC_POD="$(master_pod "${E2E_KIVIDB_NS}" "${NAME}")"
if [[ -n "${SRC_POD}" ]]; then
  kill_pod "${E2E_KIVIDB_NS}" "${SRC_POD}"
else
  warn "no master pod found to kill during chaos A"
fi

# Job should eventually fail or retry (backoffLimit=2 on the CronJob template).
wait_for_condition 240 "Job ${JOB_A} completed or failed" \
  bash -c '
    ns="$1"; job="$2"
    succ=$(kubectl get job -n "$ns" "$job" -o jsonpath="{.status.succeeded}" 2>/dev/null || echo 0)
    fail=$(kubectl get job -n "$ns" "$job" -o jsonpath="{.status.failed}" 2>/dev/null || echo 0)
    [[ "${succ:-0}" -ge 1 || "${fail:-0}" -ge 1 ]]
  ' bash "${E2E_KIVIDB_NS}" "${JOB_A}"

JOB_A_SUCC="$(kubectl get job -n "${E2E_KIVIDB_NS}" "${JOB_A}" -o jsonpath='{.status.succeeded}' 2>/dev/null || echo 0)"
JOB_A_FAIL="$(kubectl get job -n "${E2E_KIVIDB_NS}" "${JOB_A}" -o jsonpath='{.status.failed}' 2>/dev/null || echo 0)"
log "chaos A Job result: succeeded=${JOB_A_SUCC:-0} failed=${JOB_A_FAIL:-0}"

wait_cluster_phase "${E2E_KIVIDB_NS}" "${NAME}" "Running" 300
wait_pods_ready "${E2E_KIVIDB_NS}" "kividb.io/cluster=${NAME}" 240
log "cluster recovered to Running after chaos A"

# --- Chaos B: kill the backup-trigger Job pod itself ---
log "chaos B: create Job + kill backup-trigger pod"
JOB_B="${NAME}-chaos-job-$(date +%s)"
kubectl create job -n "${E2E_KIVIDB_NS}" "${JOB_B}" --from="cronjob/${CRONJOB}"
sleep 3
TRIG_POD="$(kubectl get pods -n "${E2E_KIVIDB_NS}" -l "job-name=${JOB_B}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
if [[ -n "${TRIG_POD}" ]]; then
  kill_pod "${E2E_KIVIDB_NS}" "${TRIG_POD}"
else
  warn "backup-trigger pod for ${JOB_B} not found yet"
fi

wait_for_condition 240 "Job ${JOB_B} completed or failed" \
  bash -c '
    ns="$1"; job="$2"
    succ=$(kubectl get job -n "$ns" "$job" -o jsonpath="{.status.succeeded}" 2>/dev/null || echo 0)
    fail=$(kubectl get job -n "$ns" "$job" -o jsonpath="{.status.failed}" 2>/dev/null || echo 0)
    [[ "${succ:-0}" -ge 1 || "${fail:-0}" -ge 1 ]]
  ' bash "${E2E_KIVIDB_NS}" "${JOB_B}"

JOB_B_SUCC="$(kubectl get job -n "${E2E_KIVIDB_NS}" "${JOB_B}" -o jsonpath='{.status.succeeded}' 2>/dev/null || echo 0)"
JOB_B_FAIL="$(kubectl get job -n "${E2E_KIVIDB_NS}" "${JOB_B}" -o jsonpath='{.status.failed}' 2>/dev/null || echo 0)"
log "chaos B Job result: succeeded=${JOB_B_SUCC:-0} failed=${JOB_B_FAIL:-0}"

# Next scheduled / manual snapshot should be able to succeed after recovery.
wait_cluster_phase "${E2E_KIVIDB_NS}" "${NAME}" "Running" 300
BEFORE_OK="$(kubectl get kdbs -n "${E2E_KIVIDB_NS}" -l "kividb.io/cluster=${NAME}" \
  -o jsonpath='{range .items[?(@.status.phase=="Succeeded")]}{.metadata.name}{"\n"}{end}' 2>/dev/null | wc -l | tr -d ' ')"

JOB_C="${NAME}-recover-$(date +%s)"
kubectl create job -n "${E2E_KIVIDB_NS}" "${JOB_C}" --from="cronjob/${CRONJOB}"
wait_for_condition 240 "recovery Job ${JOB_C} succeeded" \
  bash -c '
    ns="$1"; job="$2"
    succ=$(kubectl get job -n "$ns" "$job" -o jsonpath="{.status.succeeded}" 2>/dev/null || echo 0)
    [[ "${succ:-0}" -ge 1 ]]
  ' bash "${E2E_KIVIDB_NS}" "${JOB_C}"

AFTER_OK="$(kubectl get kdbs -n "${E2E_KIVIDB_NS}" -l "kividb.io/cluster=${NAME}" \
  -o jsonpath='{range .items[?(@.status.phase=="Succeeded")]}{.metadata.name}{"\n"}{end}' 2>/dev/null | wc -l | tr -d ' ')"
log "Succeeded KividbSnapshots before=${BEFORE_OK} after=${AFTER_OK}"

# Print status fields for reporting.
log "KividbSnapshot status dump:"
kubectl get kdbs -n "${E2E_KIVIDB_NS}" -l "kividb.io/cluster=${NAME}" -o yaml | \
  awk '/^(apiVersion|kind|  name:|  phase:|  objectKey:|  sizeBytes:|  durationMs:|  sourcePod:|  sourceRole:|  error:|  startTime:|  completionTime:)/{print}' \
  || kubectl get kdbs -n "${E2E_KIVIDB_NS}" -l "kividb.io/cluster=${NAME}" -o wide

# Verify object exists in MinIO for at least one successful snapshot.
OBJ_KEY="$(kubectl get kdbs -n "${E2E_KIVIDB_NS}" -l "kividb.io/cluster=${NAME}" \
  -o jsonpath='{range .items[?(@.status.phase=="Succeeded")]}{.status.objectKey}{"\n"}{end}' 2>/dev/null | head -n1 || true)"
if [[ -z "${OBJ_KEY}" ]]; then
  die "no Succeeded snapshot objectKey found"
fi
log "checking MinIO for objectKey=${OBJ_KEY}"
kubectl delete job -n "${E2E_NS}" minio-verify-object --ignore-not-found >/dev/null 2>&1 || true
kubectl apply -n "${E2E_NS}" -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: minio-verify-object
spec:
  ttlSecondsAfterFinished: 60
  backoffLimit: 2
  template:
    spec:
      restartPolicy: OnFailure
      containers:
        - name: mc
          image: quay.io/minio/mc:latest
          imagePullPolicy: IfNotPresent
          env:
            - name: MINIO_ROOT_USER
              valueFrom:
                secretKeyRef:
                  name: minio-creds
                  key: rootUser
            - name: MINIO_ROOT_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: minio-creds
                  key: rootPassword
          command:
            - /bin/sh
            - -c
            - |
              set -e
              mc alias set local http://minio.${E2E_NS}.svc.cluster.local:9000 "\$MINIO_ROOT_USER" "\$MINIO_ROOT_PASSWORD"
              mc stat "local/${MINIO_BUCKET}/${OBJ_KEY}"
          resources:
            requests:
              cpu: 25m
              memory: 32Mi
EOF

kubectl wait -n "${E2E_NS}" --for=condition=complete job/minio-verify-object --timeout=120s
log "MinIO object verified for ${OBJ_KEY}"

cleanup_cluster "${E2E_KIVIDB_NS}" "${NAME}"
kubectl delete job -n "${E2E_KIVIDB_NS}" -l "app.kubernetes.io/name=kividb-backup" --ignore-not-found || true
log "snapshot chaos suite PASS"
