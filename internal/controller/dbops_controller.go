package controller

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const dbOpsRequeue = 5 * time.Second

// dbOpsHandoverTimeout is how long the op waits for a master to hand its
// role over before it fails. The cluster controller asks a shard's
// replica for a CLUSTER FAILOVER up to maxFailoverAttempts times,
// failoverRetry apart; this leaves room for those and for the replica to
// get in sync first. The wait for the rest of the cluster to be healthy
// before a delete is bounded by it too.
const dbOpsHandoverTimeout = 5 * time.Minute

// dbOpsRestartTimeout is how long the op waits for a pod it deleted to be
// back, Ready and in sync, or for a pod to restart at all. Longer than the
// startup probe's 30 minutes (kividbStartupProbe), plus termination, a node,
// the image and a full resync of a large dataset; the rolling update waits
// for all of that too.
const dbOpsRestartTimeout = 60 * time.Minute

// DbOpsWaitingCondition is set while a restart waits: for the rest of the
// cluster to be healthy before it takes the next pod down (reason
// PodNotHealthy), for a pod it restarted to be back (PodRestarting), or
// for pods to restart (NoPods). Its LastTransitionTime is when the wait
// began.
const DbOpsWaitingCondition = "WaitingForCluster"

// DbOpsQueuedCondition is set while an op waits for another op of the same
// cluster to finish. Its waits for a handover or for the cluster start
// only once it goes on.
const DbOpsQueuedCondition = "Queued"

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

	// One op per cluster at a time: two Restart clicks in the GUI make two
	// ops, and two of them at once restart every pod twice, interleaved.
	ahead, err := r.opAhead(ctx, &op)
	if err != nil {
		return ctrl.Result{}, err
	}
	if ahead != "" {
		msg := fmt.Sprintf("waiting for KividbDbOps %s of cluster %s to finish", ahead, cluster.Name)
		if op.Status.Phase == "" {
			op.Status.Phase = kividbv1alpha1.DbOpsPending
		}
		// Its waits start over once it goes on.
		if op.Status.Restart != nil {
			op.Status.Restart.HandoverStartTime = nil
		}
		removeCondition(&op.Status.Conditions, DbOpsWaitingCondition)
		setCondition(&op.Status.Conditions, metav1.Condition{Type: DbOpsQueuedCondition, Status: metav1.ConditionTrue, Reason: "AnotherOpRunning", Message: msg})
		op.Status.Message = msg
		_ = r.Status().Update(ctx, &op)
		return ctrl.Result{RequeueAfter: dbOpsRequeue}, nil
	}
	removeCondition(&op.Status.Conditions, DbOpsQueuedCondition)

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
			msg := fmt.Sprintf("waiting for %s %s", st.CurrentPod, waiting)
			if waitedTooLong(&op, "PodRestarting", msg, dbOpsRestartTimeout) {
				return r.fail(ctx, &op, fmt.Sprintf("%s was restarted but is not back: still %s after %s", st.CurrentPod, msg, dbOpsRestartTimeout))
			}
			op.Status.Message = msg
			_ = r.Status().Update(ctx, &op)
			return ctrl.Result{RequeueAfter: dbOpsRequeue}, nil
		}
		removeCondition(&op.Status.Conditions, DbOpsWaitingCondition)
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
				if waitedTooLong(&op, "NoPods", "no pods to restart yet", dbOpsRestartTimeout) {
					return r.fail(ctx, &op, fmt.Sprintf("no pods to restart for %s", dbOpsRestartTimeout))
				}
				op.Status.Message = "no pods to restart yet"
				_ = r.Status().Update(ctx, &op)
				return ctrl.Result{RequeueAfter: dbOpsRequeue}, nil
			}
			removeCondition(&op.Status.Conditions, DbOpsWaitingCondition)
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
	// be restarted in place). The cluster controller does the handover --
	// in a sharded cluster, a CLUSTER FAILOVER within the shard; this only
	// asks for it and waits.
	if isMasterPod(&cluster, next) && hasReplica(&cluster, next) {
		// The handover has its own timer: a wait of the gate before it
		// (next became the master meanwhile) is over.
		removeCondition(&op.Status.Conditions, DbOpsWaitingCondition)
		if st.HandoverStartTime == nil {
			now := metav1.Now()
			st.HandoverStartTime = &now
		}
		if waited := time.Since(st.HandoverStartTime.Time); waited > dbOpsHandoverTimeout {
			r.withdrawStepDown(ctx, p)
			return r.fail(ctx, &op, handoverTimeoutMessage(&cluster, next))
		}
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

	st.HandoverStartTime = nil

	// The gate of reconcileRollout: every other pod is there, Ready and in
	// sync, and none is going away. Otherwise this restart takes down a
	// second pod (with another op's, or a rollout's) or the only in-sync one.
	if blocker, waiting := othersUnhealthy(podList.Items, &cluster, next); waiting != "" {
		// The only unhealthy pod, a replica still to be restarted, goes
		// first, as in reconcileRollout: the others would wait for it forever.
		if _, also := othersUnhealthy(podList.Items, &cluster, blocker); also == "" &&
			slices.Contains(st.PendingPods, blocker) && !isMasterPod(&cluster, blocker) {
			st.PendingPods = append([]string{blocker}, removeString(st.PendingPods, blocker)...)
			return ctrl.Result{RequeueAfter: dbOpsRequeue}, r.Status().Update(ctx, &op)
		}
		if waitedTooLong(&op, "PodNotHealthy", waiting, dbOpsHandoverTimeout) {
			return r.fail(ctx, &op, fmt.Sprintf("%s was not restarted: %s for more than %s", next, waiting, dbOpsHandoverTimeout))
		}
		op.Status.Message = fmt.Sprintf("waiting to restart %s: %s", next, waiting)
		_ = r.Status().Update(ctx, &op)
		return ctrl.Result{RequeueAfter: dbOpsRequeue}, nil
	}
	removeCondition(&op.Status.Conditions, DbOpsWaitingCondition)

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
	removeCondition(&op.Status.Conditions, DbOpsWaitingCondition)
	return ctrl.Result{}, r.Status().Update(ctx, op)
}

// waitedTooLong records op's wait as the WaitingForCluster condition and
// reports whether it has lasted more than limit. Each wait removes the
// condition when it ends, so LastTransitionTime is when this one began.
func waitedTooLong(op *kividbv1alpha1.KividbDbOps, reason, msg string, limit time.Duration) bool {
	setCondition(&op.Status.Conditions, metav1.Condition{Type: DbOpsWaitingCondition, Status: metav1.ConditionTrue, Reason: reason, Message: msg})
	since := meta.FindStatusCondition(op.Status.Conditions, DbOpsWaitingCondition).LastTransitionTime
	return time.Since(since.Time) > limit
}

// opAhead returns the unfinished op of op's cluster that goes before op,
// or "" when op goes now. An op that has started goes first, so one
// created the same second but listed late cannot start alongside it; then
// the oldest, by creation and name.
func (r *KividbDbOpsReconciler) opAhead(ctx context.Context, op *kividbv1alpha1.KividbDbOps) (string, error) {
	var list kividbv1alpha1.KividbDbOpsList
	if err := r.List(ctx, &list, client.InNamespace(op.Namespace)); err != nil {
		return "", err
	}
	first := op
	for i := range list.Items {
		o := &list.Items[i]
		if o.Spec.ClusterRef.Name != op.Spec.ClusterRef.Name ||
			o.Status.Phase == kividbv1alpha1.DbOpsCompleted || o.Status.Phase == kividbv1alpha1.DbOpsFailed {
			continue
		}
		if opBefore(o, first) {
			first = o
		}
	}
	if first.Name == op.Name {
		return "", nil
	}
	return first.Name, nil
}

// opBefore reports whether a goes before b in their cluster's queue.
func opBefore(a, b *kividbv1alpha1.KividbDbOps) bool {
	if aRun, bRun := a.Status.Phase == kividbv1alpha1.DbOpsRunning, b.Status.Phase == kividbv1alpha1.DbOpsRunning; aRun != bRun {
		return aRun
	}
	if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
		return a.CreationTimestamp.Before(&b.CreationTimestamp)
	}
	return a.Name < b.Name
}

// handoverTimeoutMessage is why the op failed when master did not hand
// its role over in time.
func handoverTimeoutMessage(cluster *kividbv1alpha1.KividbCluster, master string) string {
	msg := fmt.Sprintf("%s did not hand over the master role within %s, so it was not restarted", master, dbOpsHandoverTimeout)
	if isSharded(cluster) {
		return msg + fmt.Sprintf("; see the ShardSwitchover condition and events of KividbCluster %s, and the replica's log: from engine 1.2 a replica gives a manual failover up (\"Manual failover abandoned\") when it has not caught up and won its election within 5 s", cluster.Name)
	}
	return msg + fmt.Sprintf("; see the events of KividbCluster %s", cluster.Name)
}

// withdrawStepDown removes the step-down request the op set on p, so the
// cluster controller stops trying to hand its role over.
func (r *KividbDbOpsReconciler) withdrawStepDown(ctx context.Context, p *corev1.Pod) {
	if p.Annotations[StepDownAnnotation] != "true" {
		return
	}
	patch := client.MergeFrom(p.DeepCopy())
	delete(p.Annotations, StepDownAnnotation)
	if err := r.Patch(ctx, p, patch); err != nil && !apierrors.IsNotFound(err) {
		logf.FromContext(ctx).Error(err, "removing step-down annotation", "pod", p.Name)
	}
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
	case isSharded(cluster) && (cluster.Status.Cluster == nil || cluster.Status.Cluster.State != "ok"):
		return "for the cluster to serve every slot"
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

// isMasterPod reports whether pod is the master, or in a sharded cluster
// a shard's master.
func isMasterPod(cluster *kividbv1alpha1.KividbCluster, pod string) bool {
	if !isSharded(cluster) {
		return pod == cluster.Status.MasterPod
	}
	for _, sh := range cluster.Status.Shards {
		if sh.MasterPod == pod {
			return true
		}
	}
	return false
}

// hasReplica reports whether master has a replica (of its shard, in a
// sharded cluster). Whether it is in sync right now does not matter: the
// handover waits for that, up to dbOpsHandoverTimeout. Only a master with
// no replica at all (replicas: 0) is restarted in place.
func hasReplica(cluster *kividbv1alpha1.KividbCluster, master string) bool {
	var shard *int32
	for _, ps := range cluster.Status.Pods {
		if ps.Name == master {
			shard = ps.Shard
		}
	}
	for _, ps := range cluster.Status.Pods {
		if isSharded(cluster) && (shard == nil || ps.Shard == nil || *ps.Shard != *shard) {
			continue
		}
		if ps.Name != master {
			return true
		}
	}
	return false
}

// othersUnhealthy returns why next cannot be restarted yet, and the pod
// other than next that blocks it ("" when the problem is the pod count);
// why is "" when nothing blocks. All desiredPodCount pods must exist,
// and every other one must not be being deleted, and be Ready and in
// sync (Synced is shardHealthy in a sharded cluster).
func othersUnhealthy(pods []corev1.Pod, cluster *kividbv1alpha1.KividbCluster, next string) (pod, why string) {
	if want := desiredPodCount(cluster); int32(len(pods)) != want {
		return "", fmt.Sprintf("%d of %d pods exist", len(pods), want)
	}
	status := make(map[string]kividbv1alpha1.KividbPodStatus, len(cluster.Status.Pods))
	for _, ps := range cluster.Status.Pods {
		status[ps.Name] = ps
	}
	for i := range pods {
		p := &pods[i]
		switch ps, ok := status[p.Name]; {
		case p.Name == next:
		case p.DeletionTimestamp != nil:
			return p.Name, p.Name + " is being deleted"
		case !isPodReady(p) || !ok || !ps.Ready:
			return p.Name, p.Name + " is not Ready"
		case !ps.Synced:
			return p.Name, p.Name + " is not in sync"
		}
	}
	return "", ""
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
