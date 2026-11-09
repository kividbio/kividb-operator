// Package controller implements the KividbCluster reconciliation loop:
// rendering ConfigMap/Secret/StatefulSet/Service/CronJob objects from a
// KividbCluster spec (plus its referenced KividbConfig/KividbAclConfig/
// KividbSnapshotConfig), driving replication role assignment (including
// automatic failover) via the per-pod agent sidecar's HTTP API, and
// recording backup runs as KividbSnapshot objects.
package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	"github.com/robfig/cron/v3"
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

	// restartedMasters: shard masters that restarted with less than a
	// replica holds ("<ns>/<cluster>/<shard>" -> *restartedMaster), see
	// shard_restart.go.
	restartedMasters sync.Map

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
	// The status as this pass read it, which may be a version behind: see
	// writeStatus.
	read := c.Status.DeepCopy()

	if err := validateSpec(&c); err != nil {
		return ctrl.Result{}, r.rejectSpec(ctx, &c, err)
	}
	if err := validateSharded(&c); err != nil {
		return ctrl.Result{}, r.rejectSpec(ctx, &c, err)
	}

	// From here on, an input the cluster cannot run with, or a referenced
	// object that cannot be read, holds back every write but not
	// failover: see reconcileHeld.
	if err := validatePorts(&c); err != nil {
		return r.reconcileHeld(ctx, &c, heldBack("InvalidSpec", err))
	}
	kdbConfig, err := r.resolveKividbConfig(ctx, &c)
	if err != nil {
		return r.reconcileHeld(ctx, &c, heldBack("ReconcileError", fmt.Errorf("resolving configRef: %w", err)))
	}
	if err := validateKividbConfig(&c, kdbConfig); err != nil {
		return r.reconcileHeld(ctx, &c, heldBack("InvalidSpec", err))
	}
	aclConfig, err := r.resolveAclConfig(ctx, &c)
	if err != nil {
		return r.reconcileHeld(ctx, &c, heldBack("ReconcileError", fmt.Errorf("resolving aclConfigRef: %w", err)))
	}
	snapCfg, err := r.resolveSnapshotConfig(ctx, &c)
	if err != nil {
		return r.reconcileHeld(ctx, &c, heldBack("ReconcileError", fmt.Errorf("resolving snapshotConfigRef: %w", err)))
	}
	if err := validateSnapshotConfig(snapCfg); err != nil {
		return r.reconcileHeld(ctx, &c, heldBack("InvalidSpec", err))
	}

	r.emitVariantGuidance(&c, kdbConfig)

	secretValues, err := r.resolveSecretValues(ctx, &c, aclConfig)
	if err != nil {
		return r.reconcileHeld(ctx, &c, heldBack("ReconcileError", fmt.Errorf("resolving referenced secrets: %w", err)))
	}

	aclContent, err := renderACLFile(aclConfig, secretValues)
	if err != nil {
		return r.reconcileHeld(ctx, &c, heldBack("ReconcileError", fmt.Errorf("rendering ACL file: %w", err)))
	}

	var defaultPassword string
	if ref := defaultUserPasswordRef(aclConfig); ref != nil {
		defaultPassword = secretValues[secretValueKey(ref)]
	}
	// Without the auth Secret there are no generations to roll the pods by.
	generations, aclContent, err := r.reconcileSecret(ctx, &c, aclConfig, secretValues, aclContent, defaultPassword)
	if err != nil {
		return r.reconcileHeld(ctx, &c, heldBack("ReconcileError", fmt.Errorf("reconciling secret: %w", err)))
	}
	if isSharded(&c) {
		return r.reconcileSharded(ctx, &c, kdbConfig, aclConfig, snapCfg, aclContent, defaultPassword, generations, nil)
	}
	// An object the API server refuses (a backup schedule with a typo, a
	// port clash) skips the steps after it, but not failover: the error is
	// reported on the Ready condition once the roles are reconciled.
	var steps applySteps
	steps.run("reconciling config", func() error { return r.reconcileConfig(ctx, &c, kdbConfig, clusterSecrets{}) })
	steps.run("reconciling services", func() error { return r.reconcileServices(ctx, &c, kdbConfig) })

	blockSTS, bootErr := r.reconcileBootstrap(ctx, &c)
	if bootErr != nil {
		log.Error(bootErr, "bootstrap reconciliation failed")
	}
	if c.Spec.BootstrapFromSnapshot != nil {
		// The update reads the saved object back into c. Reading it again
		// would come from the cache, which may not have this write yet: a
		// bootstrap just set waiting would read as not waiting, and the
		// statefulset step below would run (issue #43).
		if statusErr := r.Status().Update(ctx, &c); statusErr != nil {
			return ctrl.Result{}, fmt.Errorf("updating bootstrap status: %w", statusErr)
		}
		read = c.Status.DeepCopy()
	}

	rollout := map[string]string{
		ConfigHashAnnotation:     configHash(renderKividbConf(&c, kdbConfig))[:16],
		AuthGenerationAnnotation: generations.auth,
	}
	// While the bootstrap waits, the StatefulSet with this name is not
	// ours. Updating it would take it over (SetControllerReference matches
	// owners by name, not UID) and scale it to 0.
	steps.run("reconciling statefulset", func() error {
		if bootstrapWaiting(&c) {
			return nil
		}
		return r.reconcileStatefulSet(ctx, &c, kdbConfig, aclConfig, snapCfg, rollout)
	})
	steps.run("reconciling disruption budget", func() error { return r.applyPDB(ctx, &c, desiredPDB(&c)) })
	steps.run("reconciling backup cronjob", func() error { return r.reconcileBackupCronJob(ctx, &c, snapCfg) })
	if snapCfg != nil {
		steps.run("reconciling snapshots", func() error { return r.reconcileSnapshots(ctx, &c, snapCfg.Name) })
	}
	applyErr := steps.err
	if applyErr != nil {
		r.warnOnChange(&c, "ReconcileError", applyErr)
	}

	if blockSTS || bootErr != nil {
		// The pass ends here, before updateStatus. The Ready condition is
		// what warnOnChange compares the next pass's error against: it
		// gives a refused step, and no longer does once that is fixed.
		if err := r.noteBootstrapApplyErr(ctx, &c, applyErr); err != nil {
			return ctrl.Result{}, err
		}
		if applyErr != nil {
			return ctrl.Result{}, applyErr
		}
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
	if applyErr != nil {
		log.Error(applyErr, "applying the cluster's objects failed")
		err = errors.Join(applyErr, err)
	}

	r.reconcileAclReload(ctx, &c, podList.Items, aclContent, generations)
	// One change of roles per pass: a pod is replaced only on a pass that
	// did not just move the master role (statuses are from before it).
	if r.reconcileHandover(ctx, &c, podList.Items, statuses, masterPod) == "" &&
		r.reconcileAuthOverlap(ctx, &c, podList.Items, generations) {
		r.reconcileRollout(ctx, &c, podList.Items, statuses, masterPod)
	}

	r.notePersistence(&c, kdbConfig)
	if statusErr := r.updateStatus(ctx, &c, read, statuses, masterPod, previousMasterPod, failoverHappened, err); errors.Is(statusErr, errStaleStatus) {
		log.V(1).Info("status changed since this pass read it; looking again")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	} else if statusErr != nil {
		return ctrl.Result{}, fmt.Errorf("updating status: %w", statusErr)
	}

	if err != nil {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	return ctrl.Result{RequeueAfter: reconcileInterval}, nil
}

// noteBootstrapApplyErr puts applyErr, from a pass that ends while a
// bootstrap waits, on the Ready condition, or, with applyErr nil, takes a
// ReconcileError an earlier such pass put there off it. The status is
// written only if that changes it. A conflict is left to the next pass.
func (r *KividbClusterReconciler) noteBootstrapApplyErr(ctx context.Context, c *kividbv1alpha1.KividbCluster, applyErr error) error {
	prev := findCondition(c.Status.Conditions, "Ready")
	// While the bootstrap waits, the cluster is not ready: the condition
	// updateStatus gives a cluster that is not, without an error.
	next := metav1.Condition{Type: "Ready", Status: metav1.ConditionFalse, Reason: "NotReady", Message: "cluster is not fully ready", ObservedGeneration: c.Generation}
	switch {
	case applyErr != nil:
		next.Reason, next.Message = "ReconcileError", applyErr.Error()
	case prev == nil || prev.Reason != "ReconcileError":
		return nil
	}
	if prev != nil && prev.Status == next.Status && prev.Reason == next.Reason && prev.Message == next.Message {
		return nil
	}
	setCondition(&c.Status.Conditions, next)
	err := r.Status().Update(ctx, c)
	switch {
	case apierrors.IsConflict(err):
		logf.FromContext(ctx).V(1).Info("Ready condition of a waiting bootstrap not written: the status changed meanwhile", "error", err.Error())
		return nil
	case err != nil:
		return errors.Join(applyErr, fmt.Errorf("updating status: %w", err))
	}
	return nil
}

// applySteps runs, in order, the steps that write a cluster's objects. The
// first that fails skips the ones after it: applying the StatefulSet after
// its config was refused would roll the pods onto a config never written.
type applySteps struct{ err error }

// run runs step, unless one before it failed. what, if set, prefixes its
// error.
func (a *applySteps) run(what string, step func() error) {
	if a.err != nil {
		return
	}
	if err := step(); err != nil && what != "" {
		a.err = fmt.Errorf("%s: %w", what, err)
	} else if err != nil {
		a.err = err
	}
}

// heldError is why a pass wrote none of the cluster's objects, with the
// reason the Ready condition gives for it.
type heldError struct {
	reason string
	err    error
}

func heldBack(reason string, err error) *heldError { return &heldError{reason: reason, err: err} }
func (e *heldError) Error() string                 { return e.err.Error() }
func (e *heldError) Unwrap() error                 { return e.err }

// reconcileHeld is a pass that writes none of the cluster's objects: a
// referenced object could not be read, or holds what the cluster cannot
// run with. Rendered without it, they would open the default user, drop
// every directive or delete the backup CronJob. The roles and handovers
// need none of it, so they still run: a dead master is still replaced, and
// the error shows on the Ready condition. The ACL reload, the password
// overlap and the rollout need what could not be resolved, so they wait.
func (r *KividbClusterReconciler) reconcileHeld(ctx context.Context, c *kividbv1alpha1.KividbCluster, held *heldError) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	read := c.Status.DeepCopy() // nothing before this pass changed it
	log.Error(held.err, "not applying the cluster's objects")
	requeue := 5 * time.Second
	r.warnOnChange(c, held.reason, held.err)
	if held.reason == "InvalidSpec" {
		// Only an edit fixes it, and edits are watched.
		requeue = reconcileInterval
	}
	if isSharded(c) {
		// Its own cadence (3 s while not Ready), not requeue: the sharded
		// pass also races a restarted master's replicas (shard_restart.go).
		return r.reconcileSharded(ctx, c, nil, nil, nil, "", "", authGenerations{}, held)
	}

	var podList corev1.PodList
	if err := r.List(ctx, &podList, client.InNamespace(c.Namespace), client.MatchingLabels(selectorLabels(c))); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing pods: %w", err)
	}
	bootstrapping := c.Spec.BootstrapFromSnapshot != nil && (c.Status.Bootstrap == nil || !c.Status.Bootstrap.Completed)
	if bootstrapping || !listensOn(podList.Items, getPort(c)) {
		// Restoring a snapshot: no pods to fail over yet. Or spec.port
		// changed, and the pods, never rolled onto it, are still on the
		// old port, while the roles would point the replicas at the new.
		setCondition(&c.Status.Conditions, metav1.Condition{
			Type: "Ready", Status: metav1.ConditionFalse, Reason: held.reason, Message: held.Error(), ObservedGeneration: c.Generation,
		})
		if err := r.Status().Update(ctx, c); err != nil {
			return ctrl.Result{}, fmt.Errorf("updating status: %w", err)
		}
		return ctrl.Result{RequeueAfter: requeue}, nil
	}

	previousMasterPod := c.Status.MasterPod
	statuses, masterPod, failoverHappened, err := r.reconcileRoles(ctx, c, podList.Items)
	if err != nil {
		log.Error(err, "role reconciliation failed")
	}
	r.reconcileHandover(ctx, c, podList.Items, statuses, masterPod)
	if statusErr := r.updateStatus(ctx, c, read, statuses, masterPod, previousMasterPod, failoverHappened, errors.Join(held, err)); errors.Is(statusErr, errStaleStatus) {
		log.V(1).Info("status changed since this pass read it; looking again")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	} else if statusErr != nil {
		return ctrl.Result{}, fmt.Errorf("updating status: %w", statusErr)
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// listensOn reports whether the kividb container of every pod declares
// port: a pod made before spec.port changed declares the old one.
func listensOn(pods []corev1.Pod, port int32) bool {
	for i := range pods {
		for _, ct := range pods[i].Spec.Containers {
			if ct.Name != kividbContainerName {
				continue
			}
			for _, p := range ct.Ports {
				if p.Name == KividbPortName && p.ContainerPort != port {
					return false
				}
			}
		}
	}
	return true
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
		if sts.ResourceVersion != "" && bootstrapBlocksSTS(c) && !metav1.IsControlledBy(sts, c) {
			// Not this cluster's (by UID): left from an earlier cluster with
			// this name, and running on the volumes the bootstrap restores.
			// SetControllerReference matches owners by name and would take
			// it over, and the garbage collector would then keep it.
			return fmt.Errorf("StatefulSet %s is not this cluster's but an earlier cluster's with this name; not updated while spec.bootstrapFromSnapshot waits to restore", sts.Name)
		}
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

func (r *KividbClusterReconciler) updateStatus(ctx context.Context, c *kividbv1alpha1.KividbCluster, read *kividbv1alpha1.KividbClusterStatus, statuses []kividbv1alpha1.KividbPodStatus, masterPod, previousMasterPod string, failoverHappened bool, roleErr error) error {
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

	// A failed reconcileRoles returns no statuses. Keep the last ones, for as
	// long as passes fail: the failover checks read them (the master's key
	// count, which replicas were in sync), and empty would switch them off.
	// This pass has no fresh status for any pod, so none is kept Ready.
	if statuses != nil || roleErr == nil {
		c.Status.Pods = statuses
	} else {
		for i := range c.Status.Pods {
			c.Status.Pods[i].Ready = false
		}
	}
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
		var held *heldError
		if errors.As(roleErr, &held) {
			readyCondition.Reason = held.reason
		}
		readyCondition.Message = roleErr.Error()
	} else if c.Status.Phase == kividbv1alpha1.PhaseRunning {
		readyCondition.Status = metav1.ConditionTrue
		readyCondition.Reason = "AllPodsReady"
		readyCondition.Message = fmt.Sprintf("master=%s, %d pod(s) ready", masterPod, len(statuses))
	}
	setCondition(&c.Status.Conditions, readyCondition)

	// A pass that read a stale status does not write over a newer one: that
	// would also take previousMasterPod from the stale status, and count a
	// failover twice.
	return r.writeStatus(ctx, c, read)
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
	if p := c.Spec.Port; p < 0 || p > 65535 { // 0: the default, 6380
		return fmt.Errorf("spec.port must be between 1 and 65535, got %d", p)
	}
	return nil
}

// validatePorts refuses a spec.port another listener of the pod has, or
// that leaves no room for the cluster bus. Unlike validateSpec's checks
// this needs no rejectSpec: the pods still listen on the port they were
// made with, so their roles can still be looked after (see reconcileHeld).
// Monitoring turned on for a cluster on 9121 is accepted by the API server.
func validatePorts(c *kividbv1alpha1.KividbCluster) error {
	p := c.Spec.Port
	if p == 0 {
		return nil
	}
	if other := portUser(c, p); other != "" {
		return fmt.Errorf("spec.port %d is also %s", p, other)
	}
	// The engine's cluster bus listens on the client port + 10000.
	if isSharded(c) && p > 65535-10000 {
		return fmt.Errorf("spec.port must be at most %d in a sharded cluster, whose bus port is spec.port + 10000, got %d", 65535-10000, p)
	}
	return nil
}

// portUser names what else in a kividb pod listens on port, "" if nothing
// does: the agent always, the exporter when monitoring is on.
func portUser(c *kividbv1alpha1.KividbCluster, port int32) string {
	switch {
	case port == AgentPort:
		return "the agent's port"
	case port == ExporterPort && c.Spec.Monitoring.Enabled:
		return "the metrics exporter's port (spec.monitoring)"
	}
	return ""
}

// validateTLSPort refuses a TLS listener port that is out of range or that
// another listener of the pod already has. Only checked when TLS is on:
// the port is not used otherwise.
func validateTLSPort(c *kividbv1alpha1.KividbCluster, kdbConfig *kividbv1alpha1.KividbConfig) error {
	tls := kividbTLSSpec(kdbConfig)
	if tls == nil || !tls.Enabled {
		return nil
	}
	port := tls.Port
	if port == 0 {
		port = 6443
	}
	switch {
	case port < 1 || port > 65535:
		return fmt.Errorf("KividbConfig %s: tls.port must be between 1 and 65535, got %d", kdbConfig.Name, port)
	case port == getPort(c):
		return fmt.Errorf("KividbConfig %s: tls.port %d is also the cluster's port (spec.port)", kdbConfig.Name, port)
	case portUser(c, port) != "":
		return fmt.Errorf("KividbConfig %s: tls.port %d is also %s", kdbConfig.Name, port, portUser(c, port))
	case isSharded(c) && port == getPort(c)+10000:
		return fmt.Errorf("KividbConfig %s: tls.port %d is also the cluster bus port (spec.port + 10000)", kdbConfig.Name, port)
	}
	return nil
}

// validateSnapshotConfig refuses a backup schedule the API server would
// refuse on the CronJob, parsed the way it parses one (robfig/cron's
// standard parser), so the typo is named here rather than by a failing
// CronJob update.
func validateSnapshotConfig(snapCfg *kividbv1alpha1.KividbSnapshotConfig) error {
	if snapCfg == nil {
		return nil
	}
	// The cron library takes a TZ= or CRON_TZ= prefix, and panics on some
	// ("TZ=UTC"); a CronJob refuses any schedule that holds "TZ"
	// (k8s.io/kubernetes, batch validation).
	if strings.Contains(snapCfg.Spec.Schedule, "TZ") {
		return fmt.Errorf("KividbSnapshotConfig %s: spec.schedule %q sets a time zone (TZ or CRON_TZ), which a CronJob does not accept", snapCfg.Name, snapCfg.Spec.Schedule)
	}
	if err := parseCronSchedule(snapCfg.Spec.Schedule); err != nil {
		return fmt.Errorf("KividbSnapshotConfig %s: spec.schedule %q is not a valid cron schedule: %w", snapCfg.Name, snapCfg.Spec.Schedule, err)
	}
	return nil
}

// parseCronSchedule parses schedule with the cron library's standard
// parser, which panics on some malformed schedules: as Kubernetes does
// (parsers.ParseCronScheduleWithPanicRecovery), the panic is an error.
func parseCronSchedule(schedule string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("invalid schedule format: %v", r)
		}
	}()
	_, err = cron.ParseStandard(schedule)
	return err
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
	if err := validateTLSPort(c, kdbConfig); err != nil {
		return err
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

// warnOnChange records err as a Warning Event of reason, unless the Ready
// condition already gives it: once per change, not every pass. A refusal
// that lasts would otherwise be an Event every few seconds, using up the
// object's Event budget (see EventCorrelatorOptions) that Failover and
// the rollout Events need. For the same reason a conflict, a write that
// raced another and that the next pass retries, is left to the condition.
// The condition's message may be err joined (one per line) with later
// errors of the pass.
func (r *KividbClusterReconciler) warnOnChange(c *kividbv1alpha1.KividbCluster, reason string, err error) {
	if apierrors.IsConflict(err) {
		return
	}
	msg := err.Error()
	if prev := findCondition(c.Status.Conditions, "Ready"); prev != nil && prev.Reason == reason &&
		(prev.Message == msg || strings.HasPrefix(prev.Message, msg+"\n")) {
		return
	}
	r.event(c, corev1.EventTypeWarning, reason, "%v", err)
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

// findCondition returns the condition of type conditionType, nil if none.
func findCondition(conditions []metav1.Condition, conditionType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}
	return nil
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
		// What a cluster references: fixing or making one is picked up at
		// once, not on the next retry.
		Watches(&kividbv1alpha1.KividbConfig{}, handler.EnqueueRequestsFromMapFunc(r.mapReferenceToClusters)).
		Watches(&kividbv1alpha1.KividbAclConfig{}, handler.EnqueueRequestsFromMapFunc(r.mapReferenceToClusters)).
		Watches(&kividbv1alpha1.KividbSnapshotConfig{}, handler.EnqueueRequestsFromMapFunc(r.mapReferenceToClusters)).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.mapReferenceToClusters)).
		Complete(r)
}

// mapReferenceToClusters enqueues the KividbClusters of obj's namespace
// that reference it: as their KividbConfig, KividbAclConfig or
// KividbSnapshotConfig, or for a Secret, as a password of their
// KividbAclConfig or as spec.cluster.busSecretRef.
func (r *KividbClusterReconciler) mapReferenceToClusters(ctx context.Context, obj client.Object) []reconcile.Request {
	var clusters kividbv1alpha1.KividbClusterList
	if err := r.List(ctx, &clusters, client.InNamespace(obj.GetNamespace())); err != nil {
		logf.FromContext(ctx).Error(err, "listing the KividbClusters that may reference an object", "namespace", obj.GetNamespace(), "name", obj.GetName())
		return nil
	}
	name := obj.GetName()
	named := func(ref *corev1.LocalObjectReference) bool { return ref != nil && ref.Name == name }
	var out []reconcile.Request
	for i := range clusters.Items {
		c := &clusters.Items[i]
		var refers bool
		switch obj.(type) {
		case *kividbv1alpha1.KividbConfig:
			refers = named(c.Spec.ConfigRef)
		case *kividbv1alpha1.KividbAclConfig:
			refers = named(c.Spec.AclConfigRef)
		case *kividbv1alpha1.KividbSnapshotConfig:
			refers = named(c.Spec.SnapshotConfigRef)
		case *corev1.Secret:
			refers = r.referencesSecret(ctx, c, name)
		}
		if refers {
			out = append(out, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(c)})
		}
	}
	return out
}

// referencesSecret reports whether c reads the Secret name itself.
func (r *KividbClusterReconciler) referencesSecret(ctx context.Context, c *kividbv1alpha1.KividbCluster, name string) bool {
	if c.Spec.Cluster != nil && c.Spec.Cluster.BusSecretRef != nil && c.Spec.Cluster.BusSecretRef.Name == name {
		return true
	}
	acl, err := r.resolveAclConfig(ctx, c)
	if err != nil {
		return false // reconciled once the KividbAclConfig is there
	}
	for _, ref := range collectSecretRefs(acl) {
		if ref.Name == name {
			return true
		}
	}
	return false
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
