// Package controller implements the KividbCluster reconciliation loop:
// rendering ConfigMap/Secret/StatefulSet/Service/CronJob objects from a
// KividbCluster spec (plus its referenced KividbConfig/KividbAclConfig/
// KividbSnapshotConfig), driving replication role assignment (including
// automatic failover) via the per-pod agent sidecar's HTTP API, and
// recording backup runs as KividbSnapshot objects.
package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// reconcileInterval bounds how stale role/health information can get
// between reconciles even when nothing changes the desired spec -- this is
// what makes failover detection actually happen instead of only running
// when someone edits the KividbCluster object.
const reconcileInterval = 10 * time.Second

// KividbClusterReconciler reconciles a KividbCluster object.
type KividbClusterReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Agent    *AgentClient
	Recorder record.EventRecorder

	// failovers: the handovers under way, by shard
	// ("<ns>/<cluster>/<shard>" -> *shardHandover).
	failovers sync.Map

	// handedOver: the last handover the operator asked a shard for
	// ("<ns>/<cluster>/<shard>" -> plannedHandover), so the move it causes
	// is reported as planned even once the old master's pod is gone.
	handedOver sync.Map

	// orphans: since when a node of a sharded cluster has been none of its
	// pods ("<ns>/<cluster>/-1/<node ID>" -> time.Time), see orphanNodes.
	orphans sync.Map

	// switchovers: when the last replicated-mode switchover of a cluster
	// ("<ns>/<cluster>" -> time.Time) was aborted, so the next one waits.
	switchovers sync.Map

	// rolloutWaits: what a cluster's rollout has been waiting for, and
	// since when ("<ns>/<cluster>" -> *rolloutWait), see rollout_wait.go.
	rolloutWaits sync.Map

	// notSynced: why each replica of a replicated-mode cluster was not in
	// sync on the last pass ("<ns>/<cluster>" -> map[pod]reason).
	notSynced sync.Map

	// clock is time.Now, unless a test sets it.
	clock func() time.Time
}

func (r *KividbClusterReconciler) scheme() *runtime.Scheme { return r.Scheme }

func (r *KividbClusterReconciler) agentToken() string {
	if r.Agent == nil {
		return ""
	}
	return r.Agent.Token
}

//+kubebuilder:rbac:groups=kividb.io,resources=kividbclusters,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=kividb.io,resources=kividbclusters/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=kividb.io,resources=kividbclusters/finalizers,verbs=update
//+kubebuilder:rbac:groups=kividb.io,resources=kividbconfigs;kividbaclconfigs;kividbsnapshotconfigs,verbs=get;list;watch
//+kubebuilder:rbac:groups=kividb.io,resources=kividbsnapshots,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=kividb.io,resources=kividbsnapshots/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=batch,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=services;configmaps;secrets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;patch;delete
//+kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile implements the main control loop for a single KividbCluster.
func (r *KividbClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var c kividbv1alpha1.KividbCluster
	if err := r.Get(ctx, req.NamespacedName, &c); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if err := validateSpec(&c); err != nil {
		return ctrl.Result{}, r.rejectSpec(ctx, &c, err)
	}
	if err := validateSharded(&c); err != nil {
		return ctrl.Result{}, r.rejectSpec(ctx, &c, err)
	}

	kdbConfig, err := r.resolveKividbConfig(ctx, &c)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("resolving configRef: %w", err)
	}
	if err := validateKividbConfig(&c, kdbConfig); err != nil {
		return ctrl.Result{}, r.rejectSpec(ctx, &c, err)
	}
	aclConfig, err := r.resolveAclConfig(ctx, &c)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("resolving aclConfigRef: %w", err)
	}
	snapCfg, err := r.resolveSnapshotConfig(ctx, &c)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("resolving snapshotConfigRef: %w", err)
	}

	r.emitVariantGuidance(&c, kdbConfig)

	secretValues, err := r.resolveSecretValues(ctx, &c, aclConfig)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("resolving referenced secrets: %w", err)
	}

	aclContent, err := renderACLFile(aclConfig, secretValues)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("rendering ACL file: %w", err)
	}

	var defaultPassword string
	if ref := defaultUserPasswordRef(aclConfig); ref != nil {
		defaultPassword = secretValues[secretValueKey(ref)]
	}
	generations, aclContent, err := r.reconcileSecret(ctx, &c, aclConfig, secretValues, aclContent, defaultPassword)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling secret: %w", err)
	}
	if isSharded(&c) {
		return r.reconcileSharded(ctx, &c, kdbConfig, aclConfig, snapCfg, aclContent, defaultPassword, generations)
	}
	if err := r.reconcileConfig(ctx, &c, kdbConfig, clusterSecrets{}); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling config: %w", err)
	}
	if err := r.reconcileServices(ctx, &c, kdbConfig); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling services: %w", err)
	}

	blockSTS, bootErr := r.reconcileBootstrap(ctx, &c)
	if bootErr != nil {
		log.Error(bootErr, "bootstrap reconciliation failed")
	}
	if c.Spec.BootstrapFromSnapshot != nil {
		if statusErr := r.Status().Update(ctx, &c); statusErr != nil {
			return ctrl.Result{}, fmt.Errorf("updating bootstrap status: %w", statusErr)
		}
		if err := r.Get(ctx, req.NamespacedName, &c); err != nil {
			return ctrl.Result{}, err
		}
	}

	rollout := map[string]string{
		ConfigHashAnnotation:     configHash(renderKividbConf(&c, kdbConfig))[:16],
		AuthGenerationAnnotation: generations.auth,
	}
	if err := r.reconcileStatefulSet(ctx, &c, kdbConfig, aclConfig, snapCfg, rollout); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling statefulset: %w", err)
	}
	if err := r.applyPDB(ctx, &c, desiredPDB(&c)); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling disruption budget: %w", err)
	}
	if err := r.reconcileBackupCronJob(ctx, &c, snapCfg); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling backup cronjob: %w", err)
	}
	if snapCfg != nil {
		if err := r.reconcileSnapshots(ctx, &c, snapCfg.Name); err != nil {
			return ctrl.Result{}, fmt.Errorf("reconciling snapshots: %w", err)
		}
	}

	if blockSTS || bootErr != nil {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// Captured before reconcileRoles/updateStatus mutate c.Status, so it
	// reflects the last *successfully persisted* master identity -- see
	// updateStatus for why this (not reconcileRoles' transient
	// failoverHappened flag alone) is what makes LastFailoverTime robust
	// to a status update that loses an optimistic-concurrency race.
	previousMasterPod := c.Status.MasterPod

	var podList corev1.PodList
	if err := r.List(ctx, &podList, client.InNamespace(c.Namespace), client.MatchingLabels(selectorLabels(&c))); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing pods: %w", err)
	}

	statuses, masterPod, failoverHappened, err := r.reconcileRoles(ctx, &c, podList.Items)
	if err != nil {
		log.Error(err, "role reconciliation failed")
		// Fall through to still persist a status update reflecting the
		// error so it is visible via `kubectl get kividbcluster`, but keep
		// requeuing quickly to retry.
	}

	r.reconcileAclReload(ctx, &c, podList.Items, aclContent, generations)
	// One change of roles per pass: a pod is replaced only on a pass that
	// did not just move the master role (statuses are from before it).
	if r.reconcileHandover(ctx, &c, podList.Items, statuses, masterPod) == "" &&
		r.reconcileAuthOverlap(ctx, &c, podList.Items, generations) {
		r.reconcileRollout(ctx, &c, podList.Items, statuses, masterPod)
	}

	if statusErr := r.updateStatus(ctx, &c, statuses, masterPod, previousMasterPod, failoverHappened, err); statusErr != nil {
		return ctrl.Result{}, fmt.Errorf("updating status: %w", statusErr)
	}

	if err != nil {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	return ctrl.Result{RequeueAfter: reconcileInterval}, nil
}

// emitVariantGuidance records Events surfacing the two ways spec.image and
// spec.variant can silently disagree, since the operator deliberately
// never derives an image tag from spec.variant (see resolveImage) or
// validates that the two actually match -- only the person who set
// spec.image knows what build it actually is. Safe to call every
// reconcile: client-go's EventRecorder aggregates repeats of the same
// (object, reason, message) into one Event with an incrementing count
// rather than creating a new object each time.
func (r *KividbClusterReconciler) emitVariantGuidance(c *kividbv1alpha1.KividbCluster, kdbConfig *kividbv1alpha1.KividbConfig) {
	if r.Recorder == nil {
		return
	}

	variant := c.Spec.Variant
	if variant != "" && variant != kividbv1alpha1.VariantStandard {
		r.Recorder.Eventf(c, corev1.EventTypeNormal, "VariantGuidance",
			"spec.variant=%s: the operator does not select or verify images by variant -- make sure spec.image (%s) is actually a %s-capable build.",
			variant, resolveImage(c), variant)
	}

	if tls := kividbTLSSpec(kdbConfig); tls != nil && tls.Enabled &&
		variant != kividbv1alpha1.VariantTLS && variant != kividbv1alpha1.VariantFull {
		r.Recorder.Event(c, corev1.EventTypeWarning, "TLSVariantMismatch",
			"the referenced KividbConfig has spec.tls.enabled: true, but spec.variant is not \"tls\" or \"full\" -- "+
				"set spec.variant accordingly and make sure spec.image points at a TLS-capable build, or the TLS listener will not come up.")
	}
}

// resolveKividbConfig fetches the KividbConfig named by spec.configRef, if
// set. Returns nil (not an error) when the ref is unset.
func (r *KividbClusterReconciler) resolveKividbConfig(ctx context.Context, c *kividbv1alpha1.KividbCluster) (*kividbv1alpha1.KividbConfig, error) {
	if c.Spec.ConfigRef == nil {
		return nil, nil
	}
	var cfg kividbv1alpha1.KividbConfig
	if err := r.Get(ctx, types.NamespacedName{Name: c.Spec.ConfigRef.Name, Namespace: c.Namespace}, &cfg); err != nil {
		return nil, fmt.Errorf("KividbConfig %q: %w", c.Spec.ConfigRef.Name, err)
	}
	return &cfg, nil
}

// resolveAclConfig fetches the KividbAclConfig named by spec.aclConfigRef,
// if set. Returns nil (not an error) when the ref is unset.
func (r *KividbClusterReconciler) resolveAclConfig(ctx context.Context, c *kividbv1alpha1.KividbCluster) (*kividbv1alpha1.KividbAclConfig, error) {
	if c.Spec.AclConfigRef == nil {
		return nil, nil
	}
	var acl kividbv1alpha1.KividbAclConfig
	if err := r.Get(ctx, types.NamespacedName{Name: c.Spec.AclConfigRef.Name, Namespace: c.Namespace}, &acl); err != nil {
		return nil, fmt.Errorf("KividbAclConfig %q: %w", c.Spec.AclConfigRef.Name, err)
	}
	return &acl, nil
}

// resolveSnapshotConfig fetches the KividbSnapshotConfig named by
// spec.snapshotConfigRef, if set. Returns nil (not an error) when the ref
// is unset -- that's how scheduled backups are disabled.
func (r *KividbClusterReconciler) resolveSnapshotConfig(ctx context.Context, c *kividbv1alpha1.KividbCluster) (*kividbv1alpha1.KividbSnapshotConfig, error) {
	if c.Spec.SnapshotConfigRef == nil {
		return nil, nil
	}
	var snap kividbv1alpha1.KividbSnapshotConfig
	if err := r.Get(ctx, types.NamespacedName{Name: c.Spec.SnapshotConfigRef.Name, Namespace: c.Namespace}, &snap); err != nil {
		return nil, fmt.Errorf("KividbSnapshotConfig %q: %w", c.Spec.SnapshotConfigRef.Name, err)
	}
	return &snap, nil
}

func (r *KividbClusterReconciler) resolveSecretValues(ctx context.Context, c *kividbv1alpha1.KividbCluster, aclConfig *kividbv1alpha1.KividbAclConfig) (map[string]string, error) {
	values := map[string]string{}
	for _, ref := range collectSecretRefs(aclConfig) {
		key := secretValueKey(ref)
		if _, ok := values[key]; ok {
			continue
		}
		var secret corev1.Secret
		if err := r.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: c.Namespace}, &secret); err != nil {
			return nil, fmt.Errorf("secret %s: %w", ref.Name, err)
		}
		v, ok := secret.Data[ref.Key]
		if !ok {
			return nil, fmt.Errorf("secret %s has no key %q", ref.Name, ref.Key)
		}
		values[key] = string(v)
	}
	return values, nil
}

// reconcileSecret writes the operator's auth Secret: the ACL file, base
// rendered with the default user's credentials kept from before a password
// change (see auth_overlap.go), and the agent token. It returns the change
// counters and the ACL file as written.
func (r *KividbClusterReconciler) reconcileSecret(ctx context.Context, c *kividbv1alpha1.KividbCluster, aclConfig *kividbv1alpha1.KividbAclConfig, secretValues map[string]string, base, defaultPassword string) (authGenerations, string, error) {
	var generations authGenerations
	aclContent := base
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secretName(c), Namespace: c.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		overlap := nextAuthOverlap(secret, base)
		aclContent = base
		if len(overlap) > 0 {
			var err error
			if aclContent, err = renderACLFileWith(aclConfig, secretValues, overlap); err != nil {
				return err
			}
		}
		desired := desiredAuthSecret(c, aclContent, r.agentToken())
		secret.Labels = desired.Labels
		secret.Type = desired.Type
		secret.StringData = desired.StringData
		secret.StringData[defaultCredentialKey] = strings.Join(aclDefaultCredentials(base), " ")
		secret.StringData[defaultOverlapKey] = strings.Join(overlap, " ")
		generations = bumpAuthGenerations(c, secret, aclContent, defaultPassword, r.agentToken())
		if len(overlap) > 0 {
			generations.overlap = overlap
			generations.defaultCredentials = aclDefaultCredentials(aclContent)
		}
		return controllerutil.SetControllerReference(c, secret, r.scheme())
	})
	return generations, aclContent, err
}

// reconcileConfig writes the rendered kividb.conf into its Secret, and
// removes the ConfigMap that held it before 0.7.0 once the Secret exists.
// (The pods move from one to the other when they next roll: the config
// volume is part of their template.)
func (r *KividbClusterReconciler) reconcileConfig(ctx context.Context, c *kividbv1alpha1.KividbCluster, kdbConfig *kividbv1alpha1.KividbConfig, secrets clusterSecrets) error {
	s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: configSecretName(c), Namespace: c.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, s, func() error {
		desired := desiredConfigSecret(c, kdbConfig, secrets)
		s.Labels = desired.Labels
		s.Type = desired.Type
		s.StringData = desired.StringData
		return controllerutil.SetControllerReference(c, s, r.scheme())
	})
	if err != nil {
		return err
	}
	return r.deleteLegacyConfigMap(ctx, c)
}

func (r *KividbClusterReconciler) deleteLegacyConfigMap(ctx context.Context, c *kividbv1alpha1.KividbCluster) error {
	var cm corev1.ConfigMap
	err := r.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: configSecretName(c)}, &cm)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !metav1.IsControlledBy(&cm, c) {
		return nil // not ours: leave it
	}
	// Kept while any pod still mounts it: a pod of the old template could
	// not start again without it. They move to the Secret as they roll.
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(c.Namespace), client.MatchingLabels(selectorLabels(c))); err != nil {
		return err
	}
	for _, p := range pods.Items {
		for _, v := range p.Spec.Volumes {
			if v.ConfigMap != nil && v.ConfigMap.Name == cm.Name {
				return nil
			}
		}
	}
	if err := r.Delete(ctx, &cm); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func (r *KividbClusterReconciler) reconcileServices(ctx context.Context, c *kividbv1alpha1.KividbCluster, kdbConfig *kividbv1alpha1.KividbConfig) error {
	builders := []func(*kividbv1alpha1.KividbCluster, *kividbv1alpha1.KividbConfig) *corev1.Service{
		desiredHeadlessService, desiredMasterService, desiredReplicaService,
	}
	for _, build := range builders {
		desired := build(c, kdbConfig)
		svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: c.Namespace}}
		_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
			svc.Labels = desired.Labels
			svc.Annotations = desired.Annotations
			svc.Spec.Type = desired.Spec.Type
			svc.Spec.Selector = desired.Spec.Selector
			svc.Spec.Ports = desired.Spec.Ports
			svc.Spec.PublishNotReadyAddresses = desired.Spec.PublishNotReadyAddresses
			if desired.Spec.ClusterIP == corev1.ClusterIPNone {
				svc.Spec.ClusterIP = corev1.ClusterIPNone
			}
			svc.Spec.LoadBalancerIP = desired.Spec.LoadBalancerIP //nolint:staticcheck
			svc.Spec.LoadBalancerSourceRanges = desired.Spec.LoadBalancerSourceRanges
			return controllerutil.SetControllerReference(c, svc, r.scheme())
		})
		if err != nil {
			return fmt.Errorf("service %s: %w", desired.Name, err)
		}
	}
	return nil
}

// rollout holds pod-template annotations whose only job is to change when
// something the pods read once at startup changes (see ConfigHashAnnotation
// and AuthGenerationAnnotation), so the StatefulSet rolls them.
func (r *KividbClusterReconciler) reconcileStatefulSet(ctx context.Context, c *kividbv1alpha1.KividbCluster, kdbConfig *kividbv1alpha1.KividbConfig, aclConfig *kividbv1alpha1.KividbAclConfig, snapCfg *kividbv1alpha1.KividbSnapshotConfig, rollout map[string]string) error {
	return r.applyStatefulSet(ctx, c, desiredStatefulSet(c, kdbConfig, aclConfig, snapCfg), rollout)
}

// applyStatefulSet creates or updates the StatefulSet desired, with the
// rollout annotations on its pod template.
func (r *KividbClusterReconciler) applyStatefulSet(ctx context.Context, c *kividbv1alpha1.KividbCluster, desired *appsv1.StatefulSet, rollout map[string]string) error {
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: c.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sts, func() error {
		creating := sts.CreationTimestamp.IsZero()
		sts.Labels = desired.Labels
		sts.Spec.Replicas = desired.Spec.Replicas
		sts.Spec.Template = desired.Spec.Template
		// A fresh map: the one on desired is c.Spec.PodAnnotations itself.
		annotations := make(map[string]string, len(desired.Spec.Template.Annotations)+len(rollout))
		for k, v := range desired.Spec.Template.Annotations {
			annotations[k] = v
		}
		for k, v := range rollout {
			annotations[k] = v
		}
		sts.Spec.Template.Annotations = annotations
		sts.Spec.UpdateStrategy = desired.Spec.UpdateStrategy
		if creating {
			// Selector, ServiceName and VolumeClaimTemplates are immutable
			// once the StatefulSet exists; only set them at creation time
			// so a later spec change (e.g. Storage.Size) never trips the
			// API server's immutable-field validation on Update.
			sts.Spec.ServiceName = desired.Spec.ServiceName
			sts.Spec.Selector = desired.Spec.Selector
			sts.Spec.VolumeClaimTemplates = desired.Spec.VolumeClaimTemplates
			sts.Spec.PodManagementPolicy = desired.Spec.PodManagementPolicy
		}
		return controllerutil.SetControllerReference(c, sts, r.scheme())
	})
	return err
}

func (r *KividbClusterReconciler) updateStatus(ctx context.Context, c *kividbv1alpha1.KividbCluster, statuses []kividbv1alpha1.KividbPodStatus, masterPod, previousMasterPod string, failoverHappened bool, roleErr error) error {
	desiredPods := c.Spec.Replicas + 1

	// reconcileRoles' own failoverHappened is a transient, single-pass
	// signal: it is only true on the exact reconcile that decided a
	// promotion was needed. If that pass's status Update then loses an
	// optimistic-concurrency race (a real, observed occurrence -- Pod
	// watch events and the periodic requeue can both trigger overlapping
	// reconciles during a failover), the retry pass typically finds
	// everything already stable (the label patch succeeded even though
	// the status write didn't) and recomputes failoverHappened=false,
	// silently losing LastFailoverTime. Comparing the newly-resolved
	// master against the *last successfully persisted* one is retry-safe:
	// previousMasterPod only changes once an Update actually lands, so
	// this comparison keeps returning true on every retry until it does.
	masterChanged := previousMasterPod != "" && masterPod != "" && previousMasterPod != masterPod
	failoverObserved := failoverHappened || masterChanged
	logf.FromContext(ctx).V(1).Info("updateStatus", "previousMasterPod", previousMasterPod, "masterPod", masterPod, "failoverHappened", failoverHappened, "masterChanged", masterChanged, "failoverObserved", failoverObserved)

	c.Status.Pods = statuses
	c.Status.MasterPod = masterPod
	c.Status.ObservedGeneration = c.Generation
	c.Status.Phase = computePhase(desiredPods, statuses, masterPod, failoverObserved)

	if failoverObserved {
		now := metav1.Now()
		c.Status.LastFailoverTime = &now
	}

	readyCondition := metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionFalse,
		Reason:             "NotReady",
		Message:            "cluster is not fully ready",
		ObservedGeneration: c.Generation,
	}
	if roleErr != nil {
		readyCondition.Reason = "ReconcileError"
		readyCondition.Message = roleErr.Error()
	} else if c.Status.Phase == kividbv1alpha1.PhaseRunning {
		readyCondition.Status = metav1.ConditionTrue
		readyCondition.Reason = "AllPodsReady"
		readyCondition.Message = fmt.Sprintf("master=%s, %d pod(s) ready", masterPod, len(statuses))
	}
	setCondition(&c.Status.Conditions, readyCondition)

	return r.Status().Update(ctx, c)
}

// validateSpec catches spec values the API server accepted but that the
// rest of the reconcile cannot work with. CRD schema validation covers the
// same ground for new objects, but Helm never upgrades CRDs, so a cluster
// can well be running this operator against an older, laxer schema.
func validateSpec(c *kividbv1alpha1.KividbCluster) error {
	if _, err := resource.ParseQuantity(c.Spec.Storage.Size); err != nil {
		return fmt.Errorf("spec.storage.size %q is not a valid quantity (e.g. \"10Gi\"): %w", c.Spec.Storage.Size, err)
	}
	if c.Spec.Replicas < 0 {
		return fmt.Errorf("spec.replicas must not be negative, got %d", c.Spec.Replicas)
	}
	return nil
}

// validateKividbConfig refuses directives the operator cannot run with.
// cluster-enabled turns on kividb's cluster mode, whose failover the
// operator's own (REPLICAOF-driven) failover would fight: the replicas
// would be refused, roles mislabelled and rollouts stuck. Sharded clusters
// are spec.shards, not a directive. In a sharded cluster, the operator
// sets every cluster-mode directive itself.
func validateKividbConfig(c *kividbv1alpha1.KividbCluster, kdbConfig *kividbv1alpha1.KividbConfig) error {
	if kdbConfig == nil {
		return nil
	}
	if isSharded(c) {
		var owned []string
		for k := range kdbConfig.Spec.Directives {
			if operatorOwnedClusterDirective(k) {
				owned = append(owned, k)
			}
		}
		if len(owned) > 0 {
			sort.Strings(owned)
			return fmt.Errorf("KividbConfig %s sets %s: in a sharded cluster the operator sets these itself (see spec.cluster)", kdbConfig.Name, strings.Join(owned, ", "))
		}
		return nil
	}
	for k, v := range kdbConfig.Spec.Directives {
		if strings.EqualFold(k, "cluster-enabled") && !strings.EqualFold(strings.TrimSpace(v), "no") {
			return fmt.Errorf("KividbConfig %s sets cluster-enabled %q: cluster mode is not supported through a directive", kdbConfig.Name, v)
		}
	}
	return nil
}

// rejectSpec surfaces an invalid spec on the object itself and stops:
// nothing is requeued, since only an edit to the KividbCluster (which
// triggers a reconcile on its own) can fix it.
func (r *KividbClusterReconciler) rejectSpec(ctx context.Context, c *kividbv1alpha1.KividbCluster, specErr error) error {
	logf.FromContext(ctx).Error(specErr, "invalid KividbCluster spec")
	r.event(c, corev1.EventTypeWarning, "InvalidSpec", "%v", specErr)

	c.Status.ObservedGeneration = c.Generation
	c.Status.Phase = kividbv1alpha1.PhaseError
	setCondition(&c.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionFalse,
		Reason:             "InvalidSpec",
		Message:            specErr.Error(),
		ObservedGeneration: c.Generation,
	})
	return r.Status().Update(ctx, c)
}

func setCondition(conditions *[]metav1.Condition, next metav1.Condition) {
	next.LastTransitionTime = metav1.Now()
	for i, existing := range *conditions {
		if existing.Type == next.Type {
			if existing.Status == next.Status {
				next.LastTransitionTime = existing.LastTransitionTime
			}
			(*conditions)[i] = next
			return
		}
	}
	*conditions = append(*conditions, next)
}

func removeCondition(conditions *[]metav1.Condition, conditionType string) {
	kept := (*conditions)[:0]
	for _, existing := range *conditions {
		if existing.Type != conditionType {
			kept = append(kept, existing)
		}
	}
	*conditions = kept
}

// EventCorrelatorOptions is how the operator's Events are rate-limited.
// client-go's defaults let 25 through per object and then one every 5
// minutes: one rollout of a sharded cluster (a RollingUpdate per pod, a
// ShardSwitchover per master) used that up, and what came after it --
// PasswordChanged at the end of a password change, in the AKS run -- was
// silently dropped. These let 100 through, then one every 5 seconds; the
// correlator still folds similar Events together.
func EventCorrelatorOptions() record.CorrelatorOptions {
	return record.CorrelatorOptions{BurstSize: 100, QPS: 1.0 / 5}
}

// SetupWithManager wires the controller into the manager, watching
// KividbCluster objects plus the child objects it owns so external edits
// (e.g. `kubectl edit statefulset`) get reconciled back to the desired
// state, and Pod status changes trigger prompt failover checks.
func (r *KividbClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&kividbv1alpha1.KividbCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Secret{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Owns(&batchv1.CronJob{}).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(mapPodToCluster)).
		Watches(&corev1.Service{}, handler.EnqueueRequestsFromMapFunc(mapAnnounceServiceToCluster)).
		Complete(r)
}

// mapPodToCluster enqueues a reconcile for the KividbCluster named by a
// Pod's ClusterLabel, so readiness/status changes on individual pods (not
// just changes to the StatefulSet object itself) promptly trigger failover
// evaluation instead of waiting for the next periodic requeue.
func mapPodToCluster(_ context.Context, obj client.Object) []reconcile.Request {
	name, ok := obj.GetLabels()[kividbv1alpha1.ClusterLabel]
	if !ok || name == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: obj.GetNamespace(), Name: name}}}
}
