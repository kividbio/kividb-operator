package controller

import (
	"context"
	"fmt"
	"sort"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// Restarting a sharded cluster's pods: a template change, a new password,
// a KividbDbOps restart. As with one replicated set (see reconcileRollout),
// one pod at a time, replicas first, and only while everything else is
// healthy. A shard's master is never restarted while it is the master: a
// step-down asks its most caught-up replica for a CLUSTER FAILOVER, which
// the engine runs without losing a write (the master pauses its clients
// until the replica has caught up), and the old master is restarted as a
// replica afterwards. A shard with no replicas has no one to hand over
// to, and its master is restarted in place: its slots are down until it
// is back.

// failoverRetry is how long a CLUSTER FAILOVER the operator asked for is
// given before it is asked for again.
const failoverRetry = 20 * time.Second

// shardSynced reports whether replica p follows its shard's master m and
// has caught up with it (the same rule as replicaSynced).
func shardSynced(p, m *shardPod) bool {
	if p == nil || m == nil || p == m || p.status == nil || m.status == nil {
		return false
	}
	me, ok := p.myself()
	mm, mok := m.myself()
	if !ok || !mok || me.IsMaster() || me.MasterID != mm.ID || !p.status.MasterLinkUp {
		return false
	}
	if m.status.ReplicationOffset > 0 {
		return p.status.ReplicationOffset > 0 && m.status.ReplicationOffset-p.status.ReplicationOffset <= maxSyncLagBytes
	}
	return true
}

// shardHealthy is a pod that serves its part: a master owning its
// shard's slots, or a replica in sync with it.
func shardHealthy(v clusterView, p *shardPod) bool {
	if !p.ready || p.status == nil {
		return false
	}
	m := v.masters[p.shard]
	return m == p || shardSynced(p, m)
}

// reconcileShardStepDown runs the CLUSTER FAILOVER a StepDownAnnotation on
// a shard master's pod asks for, on the shard's most caught-up replica in
// sync. The annotation is removed by clearShardStepDowns once the pod is
// a replica. It reports whether it asked for a failover.
func (r *KividbClusterReconciler) reconcileShardStepDown(ctx context.Context, c *kividbv1alpha1.KividbCluster, pods []*shardPod) bool {
	log := logf.FromContext(ctx)
	v := buildView(c, pods)
	acted := false
	for _, p := range pods {
		if p.pod.Annotations[StepDownAnnotation] != "true" || p.pod.DeletionTimestamp != nil || v.masters[p.shard] != p {
			continue
		}
		var best *shardPod
		for _, q := range pods {
			if q.shard == p.shard && q.ready && shardSynced(q, p) && (best == nil || q.status.ReplicationOffset > best.status.ReplicationOffset) {
				best = q
			}
		}
		if best == nil {
			log.V(1).Info("step-down requested, but no replica of the shard is in sync yet", "pod", p.pod.Name)
			continue
		}
		key := fmt.Sprintf("%s/%s/%d", c.Namespace, c.Name, p.shard)
		if at, ok := r.failovers.Load(key); ok && time.Since(at.(time.Time)) < failoverRetry {
			continue // the engine is still on it
		}
		if err := r.Agent.ClusterFailover(ctx, best.ip()); err != nil {
			log.Error(err, "asking for CLUSTER FAILOVER", "pod", best.pod.Name)
			continue
		}
		r.failovers.Store(key, time.Now())
		r.event(c, corev1.EventTypeNormal, "ShardSwitchover", "shard %d: asked %s to take over from %s (CLUSTER FAILOVER)", p.shard, best.pod.Name, p.pod.Name)
		acted = true
	}
	return acted
}

// clearShardStepDowns removes the step-down request from a pod that is no
// longer a master: the request is fulfilled.
func (r *KividbClusterReconciler) clearShardStepDowns(ctx context.Context, pods []*shardPod) {
	for _, p := range pods {
		if p.pod.Annotations[StepDownAnnotation] != "true" || roleOf(p) != kividbv1alpha1.RoleReplica {
			continue
		}
		patch := client.MergeFrom(p.pod.DeepCopy())
		delete(p.pod.Annotations, StepDownAnnotation)
		if err := r.Patch(ctx, p.pod, patch); err != nil && !apierrors.IsNotFound(err) {
			logf.FromContext(ctx).Error(err, "removing step-down annotation", "pod", p.pod.Name)
		}
	}
}

// requestStepDown sets the StepDownAnnotation on pod.
func (r *KividbClusterReconciler) requestStepDown(ctx context.Context, pod *corev1.Pod) error {
	if pod.Annotations[StepDownAnnotation] == "true" {
		return nil
	}
	patch := client.MergeFrom(pod.DeepCopy())
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[StepDownAnnotation] = "true"
	return r.Patch(ctx, pod, patch)
}

// reconcileShardedRollout moves the pods onto their shard StatefulSet's
// current template (OnDelete), one at a time across the whole cluster.
// It reports whether it replaced a pod or asked for a step-down.
func (r *KividbClusterReconciler) reconcileShardedRollout(ctx context.Context, c *kividbv1alpha1.KividbCluster, pods []*shardPod) bool {
	log := logf.FromContext(ctx)

	targets := map[int32]string{}
	for s := int32(0); s < shardCount(c); s++ {
		var sts appsv1.StatefulSet
		if err := r.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: shardStatefulSetName(c, s)}, &sts); err != nil {
			if !apierrors.IsNotFound(err) {
				log.Error(err, "rollout: reading StatefulSet", "shard", s)
			}
			return false
		}
		if sts.Status.UpdateRevision == "" || sts.Status.ObservedGeneration < sts.Generation {
			return false // the StatefulSet controller has not seen the latest template yet
		}
		targets[s] = sts.Status.UpdateRevision
	}

	var outdated []*shardPod
	for _, p := range pods {
		if p.pod.DeletionTimestamp != nil {
			return false // one replacement at a time
		}
		if p.pod.Labels[appsv1.StatefulSetRevisionLabel] != targets[p.shard] {
			outdated = append(outdated, p)
		}
	}
	if len(outdated) == 0 {
		return false
	}

	// A pod serving nothing has nothing to protect.
	replaced := false
	for _, p := range outdated {
		if p.ready || time.Since(unreadySince(p.pod)) < stuckPodGrace {
			continue
		}
		log.Info("rollout: replacing unready pod on an outdated template", "pod", p.pod.Name)
		if err := r.Delete(ctx, p.pod); err != nil && !apierrors.IsNotFound(err) {
			log.Error(err, "rollout: deleting pod", "pod", p.pod.Name)
			continue
		}
		r.event(c, corev1.EventTypeWarning, "StuckPodReplaced", "%s was not ready and on an outdated pod template; replaced it", p.pod.Name)
		replaced = true
	}
	if replaced {
		return true
	}

	// The rest only while the cluster is whole and serving every slot.
	v := buildView(c, pods)
	if int32(len(pods)) != shardCount(c)*(c.Spec.Replicas+1) || !v.formed || v.anchor == nil || v.anchor.status.State != "ok" {
		return false
	}
	healthy := map[*shardPod]bool{}
	unhealthy := 0
	for _, p := range pods {
		healthy[p] = shardHealthy(v, p)
		if !healthy[p] {
			unhealthy++
		}
	}
	isMaster := func(p *shardPod) bool { return v.masters[p.shard] == p }
	// Replicas first, masters last; among them, one that is not healthy
	// first (it is the least use as it stands), then by name, highest
	// first.
	sort.Slice(outdated, func(i, j int) bool {
		a, b := outdated[i], outdated[j]
		if isMaster(a) != isMaster(b) {
			return isMaster(b)
		}
		if healthy[a] != healthy[b] {
			return !healthy[a]
		}
		return a.pod.Name > b.pod.Name
	})
	next := outdated[0]
	// Every pod but the one replaced must be healthy (see reconcileRollout).
	if unhealthy > 1 || (unhealthy == 1 && healthy[next]) {
		return false
	}

	if isMaster(next) && c.Spec.Replicas > 0 {
		if err := r.requestStepDown(ctx, next.pod); err != nil {
			log.Error(err, "rollout: asking the shard master to step down", "pod", next.pod.Name)
			return false
		}
		return r.reconcileShardStepDown(ctx, c, pods)
	}
	log.Info("rollout: replacing pod to apply the updated pod template", "pod", next.pod.Name, "remaining", len(outdated)-1)
	if err := r.Delete(ctx, next.pod); err != nil && !apierrors.IsNotFound(err) {
		log.Error(err, "rollout: deleting pod", "pod", next.pod.Name)
		return false
	}
	r.event(c, corev1.EventTypeNormal, "RollingUpdate", "replacing %s to apply the updated pod template (%d more to go)", next.pod.Name, len(outdated)-1)
	return true
}
