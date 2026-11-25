package controller

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	// ConfigInUseFinalizer is held on a KividbConfig, KividbAclConfig or
	// KividbSnapshotConfig while status.usedBy is non-empty, so that deleting
	// one that KividbClusters still reference leaves it Terminating until
	// none do, instead of removing it from under them.
	ConfigInUseFinalizer = "kividb.io/in-use"

	// ConditionDeletionBlocked is True on a config that is being deleted
	// while KividbClusters still reference it.
	ConditionDeletionBlocked = "DeletionBlocked"

	// ReasonInUse is the reason of ConditionDeletionBlocked and of the
	// Warning Event emitted with it.
	ReasonInUse = "InUse"
)

// ConfigUsageReconciler computes status.usedBy on shared configuration objects
// (KividbConfig, KividbAclConfig, KividbSnapshotConfig) by listing KividbClusters
// via field indexes, and guards their deletion with ConfigInUseFinalizer while
// usedBy is non-empty.
type ConfigUsageReconciler[T client.Object] struct {
	client.Client
	Scheme             *runtime.Scheme
	ControllerName     string
	NewObject          func() T
	IndexField         string
	GetUsedBy          func(T) []string
	SetUsedBy          func(T, []string)
	ExtractClusterRefs func(*kividbv1alpha1.KividbCluster) []string
	// ConditionsOf returns the config's status.conditions, for
	// ConditionDeletionBlocked. Optional: without it no condition is set.
	ConditionsOf func(T) *[]metav1.Condition
	// Recorder emits the Warning Event of a blocked deletion. Optional.
	Recorder record.EventRecorder
	// BootstrapUsers, for KividbSnapshotConfig, also counts as a user each
	// cluster whose bootstrap is not done and whose snapshot this config
	// made: the restore Job is made from the snapshot's config, not from
	// the cluster's spec.snapshotConfigRef, and every pass of the
	// bootstrap reads it until the restore is recorded.
	BootstrapUsers bool
}

//+kubebuilder:rbac:groups=kividb.io,resources=kividbconfigs;kividbaclconfigs;kividbsnapshotconfigs,verbs=get;list;watch;update;patch
//+kubebuilder:rbac:groups=kividb.io,resources=kividbconfigs/status;kividbaclconfigs/status;kividbsnapshotconfigs/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=kividb.io,resources=kividbconfigs/finalizers;kividbaclconfigs/finalizers;kividbsnapshotconfigs/finalizers,verbs=update
//+kubebuilder:rbac:groups=kividb.io,resources=kividbclusters,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile calculates the list of clusters referencing this config and updates status.usedBy.
func (r *ConfigUsageReconciler[T]) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	obj := r.NewObject()
	if err := r.Get(ctx, req.NamespacedName, obj); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	var clusters kividbv1alpha1.KividbClusterList
	if err := r.List(ctx, &clusters, client.InNamespace(req.Namespace), client.MatchingFields{r.IndexField: req.Name}); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing clusters referencing %s: %w", req.Name, err)
	}

	var usedBy []string
	for _, c := range clusters.Items {
		if c.DeletionTimestamp == nil {
			usedBy = append(usedBy, c.Name)
		}
	}
	if r.BootstrapUsers {
		restoring, err := r.bootstrapUsers(ctx, req.Namespace, req.Name)
		if err != nil {
			return ctrl.Result{}, err
		}
		for _, name := range restoring {
			if !slices.Contains(usedBy, name) {
				usedBy = append(usedBy, name)
			}
		}
	}
	sort.Strings(usedBy)

	// The finalizer: held while any cluster references the config, let go
	// of once none does. A cluster being deleted no longer counts, so
	// deleting a whole namespace does not hang on it: its clusters go, usedBy
	// empties, and the finalizer comes off. None is added to a config that
	// is already being deleted (the API refuses it).
	deleting := obj.GetDeletionTimestamp() != nil
	held := controllerutil.ContainsFinalizer(obj, ConfigInUseFinalizer)
	switch {
	case len(usedBy) > 0 && !held && !deleting:
		if err := r.setFinalizer(ctx, req.NamespacedName, true); err != nil {
			return ctrl.Result{}, err
		}
	case len(usedBy) == 0 && held:
		if err := r.setFinalizer(ctx, req.NamespacedName, false); err != nil {
			return ctrl.Result{}, err
		}
		if deleting {
			log.Info("config no longer in use: its deletion goes ahead", "name", req.Name)
			return ctrl.Result{}, nil
		}
	}

	blocked := deleting && len(usedBy) > 0
	existing := r.GetUsedBy(obj)
	if !blocked && len(existing) == 0 && len(usedBy) == 0 {
		return ctrl.Result{}, nil
	}

	var newlyBlocked bool
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := r.NewObject()
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		changed := !slices.Equal(r.GetUsedBy(latest), usedBy)
		r.SetUsedBy(latest, usedBy)
		newlyBlocked = false
		if blocked && r.ConditionsOf != nil {
			newlyBlocked = meta.SetStatusCondition(r.ConditionsOf(latest), metav1.Condition{
				Type:               ConditionDeletionBlocked,
				Status:             metav1.ConditionTrue,
				Reason:             ReasonInUse,
				Message:            blockedMessage(usedBy),
				ObservedGeneration: latest.GetGeneration(),
			})
			changed = changed || newlyBlocked
		}
		if !changed {
			return nil
		}
		return r.Status().Update(ctx, latest)
	})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("updating status of %s: %w", req.Name, err)
	}
	// Once each time the clusters it waits for change, not on every
	// reconcile.
	if blocked && (newlyBlocked || r.ConditionsOf == nil) && r.Recorder != nil {
		r.Recorder.Event(obj, corev1.EventTypeWarning, ReasonInUse, blockedMessage(usedBy))
	}

	log.V(1).Info("updated config status.usedBy", "name", req.Name, "usedBy", usedBy, "deletionBlocked", blocked)
	return ctrl.Result{}, nil
}

// bootstrapUsers names the clusters in namespace, not being deleted, whose
// bootstrap is not done and restores a snapshot that the
// KividbSnapshotConfig name made. A snapshot not found is no user: the
// bootstrap fails on it before it reads any config.
func (r *ConfigUsageReconciler[T]) bootstrapUsers(ctx context.Context, namespace, name string) ([]string, error) {
	var clusters kividbv1alpha1.KividbClusterList
	if err := r.List(ctx, &clusters, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("listing clusters bootstrapping from a snapshot of %s: %w", name, err)
	}
	var users []string
	for i := range clusters.Items {
		c := &clusters.Items[i]
		if c.DeletionTimestamp != nil {
			continue
		}
		cfg, err := bootstrapSnapshotConfigName(ctx, r.Client, c)
		if err != nil {
			return nil, err
		}
		if cfg == name {
			users = append(users, c.Name)
		}
	}
	return users, nil
}

// bootstrapSnapshotConfigName is the KividbSnapshotConfig that made the
// snapshot a bootstrap not yet done restores, or "" when there is no such
// bootstrap or its snapshot is not found.
func bootstrapSnapshotConfigName(ctx context.Context, rd client.Reader, c *kividbv1alpha1.KividbCluster) (string, error) {
	if !bootstrapPending(c) {
		return "", nil
	}
	var snap kividbv1alpha1.KividbSnapshot
	err := rd.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: c.Spec.BootstrapFromSnapshot.SnapshotRef.Name}, &snap)
	if apierrors.IsNotFound(err) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("reading snapshot %s of cluster %s's bootstrap: %w", c.Spec.BootstrapFromSnapshot.SnapshotRef.Name, c.Name, err)
	}
	return snap.Spec.SnapshotConfigRef.Name, nil
}

// clusterRefs are the configs of this kind that c uses: its own
// references and, with BootstrapUsers, the config of its bootstrap's
// snapshot.
func (r *ConfigUsageReconciler[T]) clusterRefs(ctx context.Context, c *kividbv1alpha1.KividbCluster) []string {
	refs := r.ExtractClusterRefs(c)
	if !r.BootstrapUsers {
		return refs
	}
	cfg, err := bootstrapSnapshotConfigName(ctx, r.Client, c)
	if err != nil {
		logf.FromContext(ctx).Error(err, "finding the snapshot config of a bootstrap", "cluster", c.Name)
	}
	if cfg != "" && !slices.Contains(refs, cfg) {
		refs = append(refs, cfg)
	}
	return refs
}

// blockedMessage names the clusters a config's deletion waits for: those
// that reference it and, for a KividbSnapshotConfig, those still
// bootstrapping from a snapshot it made.
func blockedMessage(usedBy []string) string {
	return fmt.Sprintf("deletion waits until no KividbCluster uses this config; still used by: %s",
		strings.Join(usedBy, ", "))
}

// setFinalizer adds (add) or removes ConfigInUseFinalizer on the config
// named nn, retrying on conflict. A config already gone is not an error.
func (r *ConfigUsageReconciler[T]) setFinalizer(ctx context.Context, nn types.NamespacedName, add bool) error {
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := r.NewObject()
		if err := r.Get(ctx, nn, latest); err != nil {
			return err
		}
		var changed bool
		if add {
			if latest.GetDeletionTimestamp() != nil {
				return nil
			}
			changed = controllerutil.AddFinalizer(latest, ConfigInUseFinalizer)
		} else {
			changed = controllerutil.RemoveFinalizer(latest, ConfigInUseFinalizer)
		}
		if !changed {
			return nil
		}
		return r.Update(ctx, latest)
	})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("updating finalizer on %s: %w", nn.Name, err)
	}
	return nil
}

// mapClusterEvents watches KividbCluster and enqueues referenced configs on create, update, or delete.
func (r *ConfigUsageReconciler[T]) mapClusterEvents() handler.EventHandler {
	return &handler.Funcs{
		CreateFunc: func(ctx context.Context, e event.CreateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			c, ok := e.Object.(*kividbv1alpha1.KividbCluster)
			if !ok {
				return
			}
			for _, ref := range r.clusterRefs(ctx, c) {
				q.Add(reconcile.Request{NamespacedName: types.NamespacedName{Namespace: c.Namespace, Name: ref}})
			}
		},
		UpdateFunc: func(ctx context.Context, e event.UpdateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			oldC, ok1 := e.ObjectOld.(*kividbv1alpha1.KividbCluster)
			newC, ok2 := e.ObjectNew.(*kividbv1alpha1.KividbCluster)
			if !ok1 || !ok2 {
				return
			}
			seen := make(map[string]bool)
			for _, ref := range append(r.clusterRefs(ctx, oldC), r.clusterRefs(ctx, newC)...) {
				if !seen[ref] {
					seen[ref] = true
					q.Add(reconcile.Request{NamespacedName: types.NamespacedName{Namespace: newC.Namespace, Name: ref}})
				}
			}
		},
		DeleteFunc: func(ctx context.Context, e event.DeleteEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			c, ok := e.Object.(*kividbv1alpha1.KividbCluster)
			if !ok {
				return
			}
			for _, ref := range r.clusterRefs(ctx, c) {
				q.Add(reconcile.Request{NamespacedName: types.NamespacedName{Namespace: c.Namespace, Name: ref}})
			}
		},
	}
}

// SetupWithManager registers the reconciler with the manager.
func (r *ConfigUsageReconciler[T]) SetupWithManager(mgr ctrl.Manager) error {
	builder := ctrl.NewControllerManagedBy(mgr).
		For(r.NewObject()).
		Watches(&kividbv1alpha1.KividbCluster{}, r.mapClusterEvents())

	if r.BootstrapUsers {
		// A cluster can be made before the snapshot it restores: once the
		// snapshot is, its config gains the cluster.
		builder = builder.Watches(&kividbv1alpha1.KividbSnapshot{},
			handler.EnqueueRequestsFromMapFunc(func(_ context.Context, obj client.Object) []reconcile.Request {
				snap, ok := obj.(*kividbv1alpha1.KividbSnapshot)
				if !ok || snap.Spec.SnapshotConfigRef.Name == "" {
					return nil
				}
				return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: snap.Namespace, Name: snap.Spec.SnapshotConfigRef.Name}}}
			}))
	}
	if r.ControllerName != "" {
		builder = builder.Named(r.ControllerName)
	}

	return builder.Complete(r)
}

// SetupConfigUsageControllers registers usage reconcilers for KividbConfig,
// KividbAclConfig, and KividbSnapshotConfig with the manager.
func SetupConfigUsageControllers(mgr ctrl.Manager) error {
	if err := (&ConfigUsageReconciler[*kividbv1alpha1.KividbConfig]{
		Client:         mgr.GetClient(),
		Scheme:         mgr.GetScheme(),
		ControllerName: "kividbconfig-usage",
		NewObject:      func() *kividbv1alpha1.KividbConfig { return &kividbv1alpha1.KividbConfig{} },
		IndexField:     ClusterConfigRefIndex,
		GetUsedBy: func(c *kividbv1alpha1.KividbConfig) []string {
			return c.Status.UsedBy
		},
		SetUsedBy: func(c *kividbv1alpha1.KividbConfig, usedBy []string) {
			c.Status.UsedBy = usedBy
		},
		ConditionsOf: func(c *kividbv1alpha1.KividbConfig) *[]metav1.Condition {
			return &c.Status.Conditions
		},
		Recorder: mgr.GetEventRecorderFor("kividbconfig-usage"),
		ExtractClusterRefs: func(c *kividbv1alpha1.KividbCluster) []string {
			if c.Spec.ConfigRef != nil && c.Spec.ConfigRef.Name != "" {
				return []string{c.Spec.ConfigRef.Name}
			}
			return nil
		},
	}).SetupWithManager(mgr); err != nil {
		return err
	}

	if err := (&ConfigUsageReconciler[*kividbv1alpha1.KividbAclConfig]{
		Client:         mgr.GetClient(),
		Scheme:         mgr.GetScheme(),
		ControllerName: "kividbaclconfig-usage",
		NewObject:      func() *kividbv1alpha1.KividbAclConfig { return &kividbv1alpha1.KividbAclConfig{} },
		IndexField:     ClusterAclConfigRefIndex,
		GetUsedBy: func(a *kividbv1alpha1.KividbAclConfig) []string {
			return a.Status.UsedBy
		},
		SetUsedBy: func(a *kividbv1alpha1.KividbAclConfig, usedBy []string) {
			a.Status.UsedBy = usedBy
		},
		ConditionsOf: func(a *kividbv1alpha1.KividbAclConfig) *[]metav1.Condition {
			return &a.Status.Conditions
		},
		Recorder: mgr.GetEventRecorderFor("kividbaclconfig-usage"),
		ExtractClusterRefs: func(c *kividbv1alpha1.KividbCluster) []string {
			if c.Spec.AclConfigRef != nil && c.Spec.AclConfigRef.Name != "" {
				return []string{c.Spec.AclConfigRef.Name}
			}
			return nil
		},
	}).SetupWithManager(mgr); err != nil {
		return err
	}

	if err := (&ConfigUsageReconciler[*kividbv1alpha1.KividbSnapshotConfig]{
		Client:         mgr.GetClient(),
		Scheme:         mgr.GetScheme(),
		ControllerName: "kividbsnapshotconfig-usage",
		NewObject:      func() *kividbv1alpha1.KividbSnapshotConfig { return &kividbv1alpha1.KividbSnapshotConfig{} },
		IndexField:     ClusterSnapshotConfigRefIndex,
		GetUsedBy: func(s *kividbv1alpha1.KividbSnapshotConfig) []string {
			return s.Status.UsedBy
		},
		SetUsedBy: func(s *kividbv1alpha1.KividbSnapshotConfig, usedBy []string) {
			s.Status.UsedBy = usedBy
		},
		ConditionsOf: func(s *kividbv1alpha1.KividbSnapshotConfig) *[]metav1.Condition {
			return &s.Status.Conditions
		},
		Recorder:       mgr.GetEventRecorderFor("kividbsnapshotconfig-usage"),
		BootstrapUsers: true,
		ExtractClusterRefs: func(c *kividbv1alpha1.KividbCluster) []string {
			if c.Spec.SnapshotConfigRef != nil && c.Spec.SnapshotConfigRef.Name != "" {
				return []string{c.Spec.SnapshotConfigRef.Name}
			}
			return nil
		},
	}).SetupWithManager(mgr); err != nil {
		return err
	}

	return nil
}
