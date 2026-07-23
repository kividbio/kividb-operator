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
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
}

func (r *KividbClusterReconciler) scheme() *runtime.Scheme { return r.Scheme }

//+kubebuilder:rbac:groups=kividb.io,resources=kividbclusters,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=kividb.io,resources=kividbclusters/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=kividb.io,resources=kividbclusters/finalizers,verbs=update
//+kubebuilder:rbac:groups=kividb.io,resources=kividbconfigs;kividbaclconfigs;kividbsnapshotconfigs,verbs=get;list;watch
//+kubebuilder:rbac:groups=kividb.io,resources=kividbsnapshots,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=kividb.io,resources=kividbsnapshots/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=batch,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=services;configmaps;secrets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;patch
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

	kdbConfig, err := r.resolveKividbConfig(ctx, &c)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("resolving configRef: %w", err)
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

	if err := r.reconcileSecret(ctx, &c, aclContent); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling secret: %w", err)
	}
	if err := r.reconcileConfigMap(ctx, &c, kdbConfig); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling configmap: %w", err)
	}
	if err := r.reconcileServices(ctx, &c); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling services: %w", err)
	}
	if err := r.reconcileStatefulSet(ctx, &c, kdbConfig, aclConfig, snapCfg); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling statefulset: %w", err)
	}
	if err := r.reconcileBackupCronJob(ctx, &c, snapCfg); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling backup cronjob: %w", err)
	}
	if snapCfg != nil {
		if err := r.reconcileSnapshots(ctx, &c, snapCfg.Name); err != nil {
			return ctrl.Result{}, fmt.Errorf("reconciling snapshots: %w", err)
		}
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

func (r *KividbClusterReconciler) reconcileSecret(ctx context.Context, c *kividbv1alpha1.KividbCluster, aclContent string) error {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secretName(c), Namespace: c.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		desired := desiredAuthSecret(c, aclContent)
		secret.Labels = desired.Labels
		secret.Type = desired.Type
		secret.StringData = desired.StringData
		return controllerutil.SetControllerReference(c, secret, r.scheme())
	})
	return err
}

func (r *KividbClusterReconciler) reconcileConfigMap(ctx context.Context, c *kividbv1alpha1.KividbCluster, kdbConfig *kividbv1alpha1.KividbConfig) error {
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: configMapName(c), Namespace: c.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		desired := desiredConfigMap(c, kdbConfig)
		cm.Labels = desired.Labels
		cm.Data = desired.Data
		return controllerutil.SetControllerReference(c, cm, r.scheme())
	})
	return err
}

func (r *KividbClusterReconciler) reconcileServices(ctx context.Context, c *kividbv1alpha1.KividbCluster) error {
	builders := []func(*kividbv1alpha1.KividbCluster) *corev1.Service{
		desiredHeadlessService, desiredMasterService, desiredReplicaService,
	}
	for _, build := range builders {
		desired := build(c)
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

func (r *KividbClusterReconciler) reconcileStatefulSet(ctx context.Context, c *kividbv1alpha1.KividbCluster, kdbConfig *kividbv1alpha1.KividbConfig, aclConfig *kividbv1alpha1.KividbAclConfig, snapCfg *kividbv1alpha1.KividbSnapshotConfig) error {
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: statefulSetName(c), Namespace: c.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sts, func() error {
		desired := desiredStatefulSet(c, kdbConfig, aclConfig, snapCfg)
		creating := sts.CreationTimestamp.IsZero()
		sts.Labels = desired.Labels
		sts.Spec.Replicas = desired.Spec.Replicas
		sts.Spec.Template = desired.Spec.Template
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
		Owns(&batchv1.CronJob{}).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(mapPodToCluster)).
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
