package controller

import (
	"context"
	"fmt"
	"slices"
	"sort"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// ConfigUsageReconciler computes status.usedBy on shared configuration objects
// (KividbConfig, KividbAclConfig, KividbSnapshotConfig) by listing KividbClusters
// via field indexes.
type ConfigUsageReconciler[T client.Object] struct {
	client.Client
	Scheme             *runtime.Scheme
	ControllerName     string
	NewObject          func() T
	IndexField         string
	GetUsedBy          func(T) []string
	SetUsedBy          func(T, []string)
	ExtractClusterRefs func(*kividbv1alpha1.KividbCluster) []string
}

//+kubebuilder:rbac:groups=kividb.io,resources=kividbconfigs;kividbaclconfigs;kividbsnapshotconfigs,verbs=get;list;watch
//+kubebuilder:rbac:groups=kividb.io,resources=kividbconfigs/status;kividbaclconfigs/status;kividbsnapshotconfigs/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=kividb.io,resources=kividbclusters,verbs=get;list;watch

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
	sort.Strings(usedBy)

	existing := r.GetUsedBy(obj)
	if len(existing) == 0 && len(usedBy) == 0 {
		return ctrl.Result{}, nil
	}
	if slices.Equal(existing, usedBy) {
		return ctrl.Result{}, nil
	}

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := r.NewObject()
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		r.SetUsedBy(latest, usedBy)
		return r.Status().Update(ctx, latest)
	})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("updating status.usedBy on %s: %w", req.Name, err)
	}

	log.V(1).Info("updated config status.usedBy", "name", req.Name, "usedBy", usedBy)
	return ctrl.Result{}, nil
}

// mapClusterEvents watches KividbCluster and enqueues referenced configs on create, update, or delete.
func (r *ConfigUsageReconciler[T]) mapClusterEvents() handler.EventHandler {
	return &handler.Funcs{
		CreateFunc: func(ctx context.Context, e event.CreateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			c, ok := e.Object.(*kividbv1alpha1.KividbCluster)
			if !ok {
				return
			}
			for _, ref := range r.ExtractClusterRefs(c) {
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
			for _, ref := range append(r.ExtractClusterRefs(oldC), r.ExtractClusterRefs(newC)...) {
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
			for _, ref := range r.ExtractClusterRefs(c) {
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
