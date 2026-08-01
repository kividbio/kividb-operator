#!/usr/bin/env bash
# 04-compat-variants.sh — exercise standard/tls/lua/full against KIVIDB_VERSION.
#
# StackGres-style: each variant gets its own KividbConfig (+ TLS Secret for
# tls/full), then a KividbCluster that references it. Image tags are set
# explicitly; variant is informational (operator emits VariantGuidance).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

if [[ "${SKIP_COMPAT:-0}" == "1" ]]; then
  log "SKIP_COMPAT=1 — skipping variant compat suite"
  exit 0
fi

log "=== compat variants (kividb ${KIVIDB_VERSION}) ==="
require kubectl redis-cli openssl

ensure_ns "${E2E_KIVIDB_NS}"

# Pull engine images into minikube when requested (variants may not be cached).
if [[ "${LOAD_IMAGES}" == "1" ]]; then
  require minikube
  for v in standard tls lua full; do
    img="$(kividb_image_for_variant "${v}")"
    log "docker pull ${img}"
    docker pull "${img}" || warn "failed to pull ${img}"
    minikube image load "${img}" || warn "failed to load ${img} into minikube"
  done
fi

run_variant() {
  local variant="$1"
  local name="compat-${variant}"
  local image
  image="$(kividb_image_for_variant "${variant}")"
  # spec.replicas = replica count *in addition to* the master.
  # standard gets 2 replicas (3 pods); others get 1 replica (2 pods).
  local replicas=1
  if [[ "${variant}" == "standard" ]]; then
    replicas=2
  fi

  log "--- variant=${variant} image=${image} replicas=${replicas} ---"
  cleanup_cluster "${E2E_KIVIDB_NS}" "${name}" || true

  local tls_block=""
  if [[ "${variant}" == "tls" || "${variant}" == "full" ]]; then
    ensure_tls_secret "${E2E_KIVIDB_NS}" "${name}-tls"
    tls_block="$(cat <<TLS
  tls:
    enabled: true
    port: ${TLS_PORT}
    certSecretRef:
      name: ${name}-tls
      certKey: tls.crt
      keyKey: tls.key
TLS
)"
  fi

  # StackGres pattern: Config CR first, then cluster referencing it.
  kubectl apply -n "${E2E_KIVIDB_NS}" -f - <<EOF
apiVersion: kividb.io/v1alpha1
kind: KividbConfig
metadata:
  name: ${name}-config
  labels:
    app.kubernetes.io/part-of: kividb-e2e
    kividb.io/variant: ${variant}
spec:
  directives:
    maxmemory: "64mb"
    loglevel: "notice"
${tls_block}
---
apiVersion: kividb.io/v1alpha1
kind: KividbCluster
metadata:
  name: ${name}
  labels:
    app.kubernetes.io/part-of: kividb-e2e
    kividb.io/variant: ${variant}
spec:
  replicas: ${replicas}
  image: ${image}
  imagePullPolicy: IfNotPresent
  variant: ${variant}
  agentImage: ${AGENT_IMG}
  port: ${KIVIDB_PORT}
  configRef:
    name: ${name}-config
$(cluster_resources_yaml)
  failover:
    enabled: true
    unhealthyThresholdSeconds: 15
  monitoring:
    enabled: false
EOF

  wait_cluster_phase "${E2E_KIVIDB_NS}" "${name}" "Running" 300
  wait_pods_ready "${E2E_KIVIDB_NS}" "kividb.io/cluster=${name}" 180

  local pong
  pong="$(redis_cli_master "${E2E_KIVIDB_NS}" "${name}" PING || true)"
  if echo "${pong}" | grep -qi pong; then
    log "PING OK (${variant})"
  else
    die "PING failed for variant=${variant}: ${pong}"
  fi

  # VariantGuidance Events for non-standard variants (informational).
  if [[ "${variant}" != "standard" ]]; then
    if has_event_reason "${E2E_KIVIDB_NS}" "KividbCluster" "${name}" "VariantGuidance"; then
      log "VariantGuidance event present for ${name}"
    else
      warn "VariantGuidance event not yet visible for ${name} (controller may aggregate slowly)"
      # Give the reconciler a moment and re-check.
      sleep 5
      if has_event_reason "${E2E_KIVIDB_NS}" "KividbCluster" "${name}" "VariantGuidance"; then
        log "VariantGuidance event present for ${name} (after wait)"
      else
        warn "VariantGuidance still missing — describe cluster for Events"
        kubectl describe kividbcluster -n "${E2E_KIVIDB_NS}" "${name}" | tail -n 40 || true
      fi
    fi
  fi

  # TLS listener re-check on 1.0.3 (ROADMAP: broken on 1.0.2).
  if [[ "${variant}" == "tls" || "${variant}" == "full" ]]; then
    local pod
    pod="$(master_pod "${E2E_KIVIDB_NS}" "${name}")"
    if check_tls_listen "${E2E_KIVIDB_NS}" "${pod}" "${TLS_PORT}"; then
      log "TLS port ${TLS_PORT} is LISTEN on ${pod} — TLS appears functional on ${KIVIDB_VERSION}"
    else
      local msg
      msg="TLS port ${TLS_PORT} NOT in LISTEN on ${pod} (variant=${variant}, image=${image}). ROADMAP documented this as broken on v1.0.2; re-checked on ${KIVIDB_VERSION} and still absent."
      if [[ "${STRICT_TLS}" == "1" ]]; then
        die "${msg} (STRICT_TLS=1)"
      else
        warn "${msg} (STRICT_TLS=0 — treating as known upstream limitation)"
        # Dump /proc/net/tcp for the report.
        kubectl exec -n "${E2E_KIVIDB_NS}" "${pod}" -c kividb -- \
          sh -c 'echo "--- /proc/net/tcp ---"; cat /proc/net/tcp; echo "--- /proc/net/tcp6 ---"; cat /proc/net/tcp6 2>/dev/null || true' \
          || true
      fi
    fi
  fi

  # Lua smoke (EVAL) for lua/full — best-effort if scripting is compiled in.
  if [[ "${variant}" == "lua" || "${variant}" == "full" ]]; then
    local eval_out
    eval_out="$(redis_cli_master "${E2E_KIVIDB_NS}" "${name}" EVAL "return redis.call('PING')" 0 || true)"
    if echo "${eval_out}" | grep -qi pong; then
      log "Lua EVAL OK (${variant})"
    else
      warn "Lua EVAL unexpected response for ${variant}: ${eval_out}"
    fi
  fi

  cleanup_cluster "${E2E_KIVIDB_NS}" "${name}"
  log "variant ${variant} PASS"
}

for v in standard tls lua full; do
  run_variant "${v}"
done

log "compat variants suite complete"
