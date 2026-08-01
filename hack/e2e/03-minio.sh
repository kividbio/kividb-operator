#!/usr/bin/env bash
# 03-minio.sh — in-cluster MinIO for KividbSnapshotConfig S3 destinations.
#
# Credentials are the well-known local defaults (minioadmin/minioadmin) —
# never commit real cloud secrets into this harness.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

if [[ "${SKIP_MINIO:-0}" == "1" ]]; then
  log "SKIP_MINIO=1 — skipping MinIO deploy"
  exit 0
fi

log "=== MinIO in ${E2E_NS} ==="
require kubectl

mkdir -p "${RESULTS_DIR}"
ensure_ns "${E2E_NS}"

kubectl apply -n "${E2E_NS}" -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: minio-creds
  labels:
    app.kubernetes.io/name: minio
    app.kubernetes.io/part-of: kividb-e2e
type: Opaque
stringData:
  rootUser: "${MINIO_ROOT_USER}"
  rootPassword: "${MINIO_ROOT_PASSWORD}"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio
  labels:
    app.kubernetes.io/name: minio
    app.kubernetes.io/part-of: kividb-e2e
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: minio
  template:
    metadata:
      labels:
        app.kubernetes.io/name: minio
        app.kubernetes.io/part-of: kividb-e2e
    spec:
      containers:
        - name: minio
          image: quay.io/minio/minio:RELEASE.2024-12-18T13-15-44Z
          imagePullPolicy: IfNotPresent
          args: ["server", "/data", "--console-address", ":9001"]
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
          ports:
            - name: api
              containerPort: 9000
            - name: console
              containerPort: 9001
          resources:
            requests:
              cpu: 50m
              memory: 128Mi
            limits:
              cpu: 250m
              memory: 256Mi
          volumeMounts:
            - name: data
              mountPath: /data
      volumes:
        - name: data
          emptyDir: {}
---
apiVersion: v1
kind: Service
metadata:
  name: minio
  labels:
    app.kubernetes.io/name: minio
    app.kubernetes.io/part-of: kividb-e2e
spec:
  type: ClusterIP
  selector:
    app.kubernetes.io/name: minio
  ports:
    - name: api
      port: 9000
      targetPort: api
    - name: console
      port: 9001
      targetPort: console
EOF

wait_pods_ready "${E2E_NS}" "app.kubernetes.io/name=minio" 180

# Create the bucket via a one-shot Job using the MinIO client (mc).
log "ensuring bucket ${MINIO_BUCKET}"
kubectl delete job -n "${E2E_NS}" minio-create-bucket --ignore-not-found >/dev/null 2>&1 || true
kubectl apply -n "${E2E_NS}" -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: minio-create-bucket
  labels:
    app.kubernetes.io/name: minio
    app.kubernetes.io/part-of: kividb-e2e
spec:
  ttlSecondsAfterFinished: 120
  backoffLimit: 3
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
              for i in 1 2 3 4 5 6 7 8 9 10; do
                if mc alias set local http://minio.${E2E_NS}.svc.cluster.local:9000 "\$MINIO_ROOT_USER" "\$MINIO_ROOT_PASSWORD"; then
                  break
                fi
                sleep 3
              done
              mc mb --ignore-existing "local/${MINIO_BUCKET}"
              mc ls "local/${MINIO_BUCKET}"
          resources:
            requests:
              cpu: 25m
              memory: 32Mi
            limits:
              cpu: 100m
              memory: 64Mi
EOF

kubectl wait -n "${E2E_NS}" --for=condition=complete job/minio-create-bucket --timeout=180s

# Also materialize the S3 credentials Secret shape expected by KividbSnapshotConfig
# into E2E_KIVIDB_NS for later suites (StackGres-style: destination config is
# separate from the cluster CR).
ensure_ns "${E2E_KIVIDB_NS}"
kubectl apply -n "${E2E_KIVIDB_NS}" -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: e2e-s3-credentials
  labels:
    app.kubernetes.io/part-of: kividb-e2e
type: Opaque
stringData:
  accessKeyId: "${MINIO_ROOT_USER}"
  secretAccessKey: "${MINIO_ROOT_PASSWORD}"
EOF

# Export for subsequent scripts in the same shell (run-all sources sequentially).
cat > "${RESULTS_DIR}/minio.env" <<EOF
export MINIO_ENDPOINT="${MINIO_ENDPOINT}"
export MINIO_BUCKET="${MINIO_BUCKET}"
export MINIO_ROOT_USER="${MINIO_ROOT_USER}"
export MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD}"
EOF

log "MinIO ready: endpoint=${MINIO_ENDPOINT} bucket=${MINIO_BUCKET}"
log "credentials exported to ${RESULTS_DIR}/minio.env (local defaults only)"
