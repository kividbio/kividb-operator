package controller

import (
	"context"
	"fmt"
	"sort"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const dbOpsRequeue = 5 * time.Second

// KividbDbOpsReconciler executes declarative database operations
// (currently: InPlace rolling restart) against a KividbCluster.
type KividbDbOpsReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=kividb.io,resources=kividbdbops,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=kividb.io,resources=kividbdbops/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=kividb.io,resources=kividbclusters,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;delete

func (r *KividbDbOpsReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var op kividbv1alpha1.KividbDbOps
	if err := r.Get(ctx, req.NamespacedName, &op); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if op.Status.Phase == kividbv1alpha1.DbOpsCompleted || op.Status.Phase == kividbv1alpha1.DbOpsFailed {
		return ctrl.Result{}, nil
	}

	if op.Spec.Op != kividbv1alpha1.DbOpsRestart {
		return r.fail(ctx, &op, fmt.Sprintf("unsupported op %q", op.Spec.Op))
	}

	var cluster kividbv1alpha1.KividbCluster
	if err := r.Get(ctx, client.ObjectKey{Namespace: op.Namespace, Name: op.Spec.ClusterRef.Name}, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return r.fail(ctx, &op, fmt.Sprintf("cluster %q not found", op.Spec.ClusterRef.Name))
		}
		return ctrl.Result{}, err
	}

	var podList corev1.PodList
	if err := r.List(ctx, &podList, client.InNamespace(op.Namespace), client.MatchingLabels(selectorLabels(&cluster))); err != nil {
		return ctrl.Result{}, err
	}

	if op.Status.Phase == "" || op.Status.Phase == kividbv1alpha1.DbOpsPending {
		now := metav1.Now()
		op.Status.Phase = kividbv1alpha1.DbOpsRunning
		op.Status.StartTime = &now
		op.Status.Message = "starting InPlace rolling restart"
		op.Status.Restart = &kividbv1alpha1.DbOpsRestartStatus{
			PendingPods: restartOrder(podList.Items, cluster.Status.MasterPod),
		}
		if err := r.Status().Update(ctx, &op); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: dbOpsRequeue}, nil
	}

	if op.Status.Restart == nil {
		op.Status.Restart = &kividbv1alpha1.DbOpsRestartStatus{}
	}
	st := op.Status.Restart

	// Wait for the pod we deleted to be replaced, become Ready and rejoin
	// the cluster before advancing.
	if st.CurrentPod != "" {
		p := findPod(podList.Items, st.CurrentPod)
		if waiting := restartPending(p, st.CurrentPodUID, &cluster); waiting != "" {
			op.Status.Message = fmt.Sprintf("waiting for %s %s", st.CurrentPod, waiting)
			_ = r.Status().Update(ctx, &op)
			return ctrl.Result{RequeueAfter: dbOpsRequeue}, nil
		}
		st.CompletedPods = appendUnique(st.CompletedPods, st.CurrentPod)
		st.PendingPods = removeString(st.PendingPods, st.CurrentPod)
		st.CurrentPod = ""
		st.CurrentPodUID = ""
		op.Status.Message = fmt.Sprintf("restarted %s", st.CompletedPods[len(st.CompletedPods)-1])
		if err := r.Status().Update(ctx, &op); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: dbOpsRequeue}, nil
	}

	if len(st.PendingPods) == 0 {
		// Refresh order if we started with no pods yet.
		if len(st.CompletedPods) == 0 {
			st.PendingPods = restartOrder(podList.Items, cluster.Status.MasterPod)
			if len(st.PendingPods) == 0 {
				op.Status.Message = "no pods to restart yet"
				_ = r.Status().Update(ctx, &op)
				return ctrl.Result{RequeueAfter: dbOpsRequeue}, nil
			}
			if err := r.Status().Update(ctx, &op); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: dbOpsRequeue}, nil
		}
		now := metav1.Now()
		op.Status.Phase = kividbv1alpha1.DbOpsCompleted
		op.Status.CompletionTime = &now
		op.Status.Message = "restart completed"
		log.Info("dbops restart completed", "cluster", cluster.Name, "pods", st.CompletedPods)
		return ctrl.Result{}, r.Status().Update(ctx, &op)
	}

	next := st.PendingPods[0]
	p := findPod(podList.Items, next)
	if p == nil {
		st.PendingPods = st.PendingPods[1:]
		op.Status.Message = fmt.Sprintf("pod %s gone; skipping", next)
		_ = r.Status().Update(ctx, &op)
		return ctrl.Result{RequeueAfter: dbOpsRequeue}, nil
	}

	// The master hands its role to an in-sync replica before it is
	// restarted (see switchover in rollout.go for why it must not simply
	// be restarted in place). The cluster controller does the handover;
	// this only asks for it and waits.
	if next == cluster.Status.MasterPod && hasSyncedReplica(&cluster, next) {
		if p.Annotations[StepDownAnnotation] != "true" {
			patch := client.MergeFrom(p.DeepCopy())
			if p.Annotations == nil {
				p.Annotations = map[string]string{}
			}
			p.Annotations[StepDownAnnotation] = "true"
			if err := r.Patch(ctx, p, patch); err != nil {
				return ctrl.Result{}, err
			}
		}
		op.Status.Message = fmt.Sprintf("waiting for %s to hand over the master role", next)
		_ = r.Status().Update(ctx, &op)
		return ctrl.Result{RequeueAfter: dbOpsRequeue}, nil
	}

	log.Info("deleting pod for restart", "pod", next, "cluster", cluster.Name)
	if err := r.Delete(ctx, p); err != nil && !apierrors.IsNotFound(err) {
		return r.fail(ctx, &op, fmt.Sprintf("deleting %s: %v", next, err))
	}
	st.CurrentPod = next
	st.CurrentPodUID = string(p.UID)
	op.Status.Message = fmt.Sprintf("restarting %s", next)
	if err := r.Status().Update(ctx, &op); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: dbOpsRequeue}, nil
}

func (r *KividbDbOpsReconciler) fail(ctx context.Context, op *kividbv1alpha1.KividbDbOps, msg string) (ctrl.Result, error) {
	now := metav1.Now()
	op.Status.Phase = kividbv1alpha1.DbOpsFailed
	op.Status.Error = msg
	op.Status.Message = msg
	op.Status.CompletionTime = &now
	return ctrl.Result{}, r.Status().Update(ctx, op)
}

func (r *KividbDbOpsReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&kividbv1alpha1.KividbDbOps{}).
		Complete(r)
}

// restartPending returns what a restart of one pod is still waiting for,
// or "" once it is done. p is the pod currently holding the restarted
// pod's name (nil if there is none) and deletedUID the UID of the pod the
// op deleted.
//
// The UID comparison is what makes this a *rolling* restart. A deleted pod
// stays in the API, still Ready, for its whole termination grace period,
// so "the pod with this name is Ready" is true the instant after the
// delete call; advancing on that takes down every pod of the cluster at
// once. The role check then holds the next deletion until the cluster
// controller has seen the replacement and re-attached it to replication.
func restartPending(p *corev1.Pod, deletedUID string, cluster *kividbv1alpha1.KividbCluster) string {
	switch {
	case p == nil:
		return "to be recreated"
	case string(p.UID) == deletedUID || p.DeletionTimestamp != nil:
		return "to terminate"
	case !isPodReady(p):
		return "to become Ready"
	}
	for _, ps := range cluster.Status.Pods {
		if ps.Name == p.Name && ps.Ready && ps.Role != kividbv1alpha1.RoleUnknown && ps.Role != "" {
			if !ps.Synced {
				return "to finish syncing from the master"
			}
			return ""
		}
	}
	return "to rejoin the cluster"
}

// hasSyncedReplica reports whether some pod other than master is a Ready,
// in-sync replica, i.e. whether the master has anyone to hand over to.
func hasSyncedReplica(cluster *kividbv1alpha1.KividbCluster, master string) bool {
	for _, ps := range cluster.Status.Pods {
		if ps.Name != master && ps.Ready && ps.Synced && ps.Role == kividbv1alpha1.RoleReplica {
			return true
		}
	}
	return false
}

// restartOrder lists replica pods first (lexicographically), then the
// master last so failover can promote a restarted replica if needed.
func restartOrder(pods []corev1.Pod, masterName string) []string {
	var replicas, masters []string
	for i := range pods {
		p := &pods[i]
		if p.DeletionTimestamp != nil {
			continue
		}
		if p.Name == masterName || p.Labels[kividbv1alpha1.RoleLabel] == string(kividbv1alpha1.RoleMaster) {
			masters = append(masters, p.Name)
			continue
		}
		replicas = append(replicas, p.Name)
	}
	sort.Strings(replicas)
	sort.Strings(masters)
	return append(replicas, masters...)
}

func findPod(pods []corev1.Pod, name string) *corev1.Pod {
	for i := range pods {
		if pods[i].Name == name {
			return &pods[i]
		}
	}
	return nil
}

func appendUnique(list []string, v string) []string {
	for _, x := range list {
		if x == v {
			return list
		}
	}
	return append(list, v)
}

func removeString(list []string, v string) []string {
	out := list[:0]
	for _, x := range list {
		if x != v {
			out = append(out, x)
		}
	}
	return out
}
