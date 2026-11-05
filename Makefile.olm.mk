# OLM bundle targets for kividb-operator. Paste into the Makefile before
# the "Misc" section, or keep this file and add `include Makefile.olm.mk`.
#
#   make bundle      generate bundle/ (manifests, metadata, tests) for VERSION
#   make scorecard   run the bundle's scorecard tests against the current cluster
#
# The base CSV is hand-maintained in config/manifests/bases. api/ has no
# operator-sdk CSV markers, so `operator-sdk generate kustomize manifests`
# is not run: it would only rewrite the base from the Go types.

# ---------------------------------------------------------------------------
# OLM bundle (OperatorHub.io: k8s-operatorhub/community-operators)
# ---------------------------------------------------------------------------

# The version k8s-operatorhub/community-operators CI validates with
# (its ci/pipeline-config-k8s.yaml, tools.operator_sdk_version).
OPERATOR_SDK_VERSION ?= v1.36.0
KUSTOMIZE_VERSION ?= v5.4.3

LOCALBIN ?= $(shell pwd)/bin
OPERATOR_SDK ?= $(LOCALBIN)/operator-sdk
KUSTOMIZE ?= $(LOCALBIN)/kustomize

BUNDLE_PACKAGE ?= kividb-operator
CHANNELS ?= alpha
DEFAULT_CHANNEL ?= alpha
BUNDLE_METADATA_OPTS ?= --channels=$(CHANNELS) --default-channel=$(DEFAULT_CHANNEL)
BUNDLE_GEN_FLAGS ?= -q --overwrite --package $(BUNDLE_PACKAGE) --version $(VERSION) $(BUNDLE_METADATA_OPTS)

# true: reference every image in the bundle by digest. Needs registry
# access while generating.
USE_IMAGE_DIGESTS ?= false
ifeq ($(USE_IMAGE_DIGESTS), true)
	BUNDLE_GEN_FLAGS += --use-image-digests
endif

.PHONY: operator-sdk
operator-sdk: ## Download operator-sdk $(OPERATOR_SDK_VERSION) into bin/ if missing.
	@if [ ! -x "$(OPERATOR_SDK)" ]; then \
		mkdir -p $(LOCALBIN); \
		OS=$$(go env GOOS) && ARCH=$$(go env GOARCH) && \
		curl -sSLo $(OPERATOR_SDK) https://github.com/operator-framework/operator-sdk/releases/download/$(OPERATOR_SDK_VERSION)/operator-sdk_$${OS}_$${ARCH} && \
		chmod +x $(OPERATOR_SDK); \
	fi

.PHONY: kustomize
kustomize: ## Install kustomize $(KUSTOMIZE_VERSION) into bin/ if missing.
	@if [ ! -x "$(KUSTOMIZE)" ]; then \
		mkdir -p $(LOCALBIN); \
		GOBIN=$(LOCALBIN) go install sigs.k8s.io/kustomize/kustomize/v5@$(KUSTOMIZE_VERSION); \
	fi

.PHONY: bundle
bundle: operator-sdk kustomize ## Generate and validate the OLM bundle for VERSION in bundle/.
	cd config/manager && $(KUSTOMIZE) edit set image docker.io/kividb/kividb-operator=$(IMG)
	$(KUSTOMIZE) build config/manifests | $(OPERATOR_SDK) generate bundle $(BUNDLE_GEN_FLAGS)
	sed -i 's|^\(    containerImage:\).*|\1 $(IMG)|' bundle/manifests/$(BUNDLE_PACKAGE).clusterserviceversion.yaml
	$(OPERATOR_SDK) bundle validate ./bundle
	$(OPERATOR_SDK) bundle validate ./bundle --select-optional suite=operatorframework

.PHONY: scorecard
scorecard: operator-sdk ## Run the bundle's scorecard tests against the current kubeconfig context.
	$(OPERATOR_SDK) scorecard ./bundle --wait-time 120s
