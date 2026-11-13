package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// steadyCase is a replicated cluster whose master and replica are both up:
// a pass changes no roles, only refreshes the status.
func steadyCase(t *testing.T, edit func(*kividbv1alpha1.KividbCluster)) *failoverCase {
	t.Helper()
	f := newFailoverCase(t, edit, nil)
	var p corev1.Pod
	if err := f.cl.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "c1-0"}, &p); err != nil {
		t.Fatal(err)
	}
	p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if err := f.cl.Update(context.Background(), &p); err != nil {
		t.Fatal(err)
	}
	return f
}

// At steady state a pass can start from a cache a version behind, before it
// has the previous pass's status write, and its own write then conflicts.
// Each was a "Reconciler error", on a growing backoff: 134 in a few hours
// (#52). It is not an error: the pass leaves the newer status alone, and
// looks again soon.
func TestReplicatedStalePassIsNotAnError(t *testing.T) {
	f := steadyCase(t, nil)
	f.wantStalePass(t, 5*time.Second)
}

// A stale pass while a failover holds the old master's writes looks again
// before that pause runs out (fencedMasterRetry), as a pass that wrote its
// status does: the next pass is the one that makes it a replica.
func TestReplicatedStalePassComesBackSoonWhileTheFencedMasterIsHeld(t *testing.T) {
	f := steadyCase(t, fencedC19)
	f.agents.status["10.0.0.9"] = masterRun(50, "stream-a", "run-9")
	f.agents.failReplicaOf = map[string]bool{"10.0.0.9": true}
	f.wantStalePass(t, fencedMasterRetry)
}

// The same for a pass that holds back the cluster's objects (reconcileHeld):
// it still runs the roles, so it is the one that demotes the fenced master.
func TestHeldStalePassComesBackSoonWhileTheFencedMasterIsHeld(t *testing.T) {
	f := steadyCase(t, func(c *kividbv1alpha1.KividbCluster) {
		fencedC19(c)
		c.Spec.SnapshotConfigRef = &corev1.LocalObjectReference{Name: "missing"}
	})
	f.agents.status["10.0.0.9"] = masterRun(50, "stream-a", "run-9")
	f.agents.failReplicaOf = map[string]bool{"10.0.0.9": true}
	f.wantStalePass(t, fencedMasterRetry)
}

// fencedC19 gives the cluster a fenced master, c1-9 at 10.0.0.9, whose
// writes a failover holds and that is not a replica yet.
func fencedC19(c *kividbv1alpha1.KividbCluster) {
	c.Status.FencedMaster = &kividbv1alpha1.FencedMaster{Name: "c1-9", PodIP: "10.0.0.9", RunID: "run-9"}
}

// wantStalePass runs a pass that reads the cluster a version behind, before
// the previous pass's status write reaches the cache, and checks that it
// leaves the newer status alone, without an error, and looks again after
// requeue.
func (f *failoverCase) wantStalePass(t *testing.T, requeue time.Duration) {
	t.Helper()
	ctx := context.Background()
	key := client.ObjectKeyFromObject(f.c)
	var stale kividbv1alpha1.KividbCluster
	if err := f.cl.Get(ctx, key, &stale); err != nil {
		t.Fatal(err)
	}
	// The previous pass's write, which the cache has not seen yet.
	var newer kividbv1alpha1.KividbCluster
	_ = f.cl.Get(ctx, key, &newer)
	newer.Status.Pods = []kividbv1alpha1.KividbPodStatus{{Name: "c1-0", Role: kividbv1alpha1.RoleMaster, Ready: true}}
	if err := f.cl.Status().Update(ctx, &newer); err != nil {
		t.Fatal(err)
	}

	served := false
	f.r.Client = interceptor.NewClient(f.cl.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if kc, ok := obj.(*kividbv1alpha1.KividbCluster); ok && !served {
				served = true
				stale.DeepCopyInto(kc)
				return nil
			}
			return cl.Get(ctx, key, obj, opts...)
		},
	})
	res, err := f.r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
	if err != nil || res.RequeueAfter != requeue {
		t.Fatalf("Reconcile = %+v, %v; want no error, and a look again in %s", res, err, requeue)
	}
	var got kividbv1alpha1.KividbCluster
	_ = f.cl.Get(ctx, key, &got)
	if got.ResourceVersion != newer.ResourceVersion {
		t.Fatalf("the stale pass wrote its status over the newer one (resourceVersion %s, want %s)", got.ResourceVersion, newer.ResourceVersion)
	}
}

// The fence is this pass's own status write (recordFence). A conflict
// after it, from a label or annotation edit, is settled by writing the
// pass's status onto the latest version, not taken for a newer pass's
// write: dropped, it would leave masterPod on the old master, and the next
// failover decides from that status.
func TestReplicatedStatusLandsAfterTheFenceAndALabelEdit(t *testing.T) {
	ctx := context.Background()
	h := newFenceHarness(t)
	read := h.cluster.Status.DeepCopy()
	previous := h.cluster.Status.MasterPod
	statuses, master, failover, err := h.r.reconcileRoles(ctx, h.cluster, read, h.pods())
	if err != nil || master != "c1-1" || !failover {
		t.Fatalf("reconcileRoles = %q, failover %v, %v; want a failover to c1-1", master, failover, err)
	}
	var edited kividbv1alpha1.KividbCluster
	if err := h.r.Get(ctx, client.ObjectKeyFromObject(h.cluster), &edited); err != nil {
		t.Fatal(err)
	}
	edited.Labels = map[string]string{"team": "a"}
	if err := h.r.Update(ctx, &edited); err != nil {
		t.Fatal(err)
	}

	if err := h.r.updateStatus(ctx, h.cluster, read, statuses, master, previous, failover, nil); err != nil {
		t.Fatalf("updateStatus: %v; want this pass's status written onto the latest version", err)
	}
	var stored kividbv1alpha1.KividbCluster
	if err := h.r.Get(ctx, client.ObjectKeyFromObject(h.cluster), &stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.MasterPod != "c1-1" || stored.Status.LastFailoverTime == nil || stored.Status.FencedMaster != nil || stored.Labels["team"] != "a" {
		t.Errorf("stored masterPod %q, lastFailoverTime %v, fencedMaster %+v, labels %v; want c1-1, set, none (it was demoted), and the label kept",
			stored.Status.MasterPod, stored.Status.LastFailoverTime, stored.Status.FencedMaster, stored.Labels)
	}
}

// Removing allow-unread-failover after its failover is a patch without a
// lock, so it goes onto a newer pass's status write too. The pass that
// removes it must not take that version for its own status write, which
// would then meet no conflict and go over the newer status.
func TestReplicatedStalePassThatClearsTheOverrideLeavesTheNewerStatus(t *testing.T) {
	ctx := context.Background()
	h := newFenceHarness(t)
	h.cluster.Annotations = map[string]string{kividbv1alpha1.AllowUnreadFailoverAnnotation: "true"}
	if err := h.r.Update(ctx, h.cluster); err != nil {
		t.Fatal(err)
	}
	key := client.ObjectKeyFromObject(h.cluster)
	var c kividbv1alpha1.KividbCluster
	if err := h.r.Get(ctx, key, &c); err != nil {
		t.Fatal(err)
	}
	read := c.Status.DeepCopy()
	// A newer pass's write, after this one read the cluster.
	var newer kividbv1alpha1.KividbCluster
	_ = h.r.Get(ctx, key, &newer)
	newer.Status.MasterPod = "c1-2"
	if err := h.r.Status().Update(ctx, &newer); err != nil {
		t.Fatal(err)
	}

	h.r.clearFailoverOverrides(ctx, &c, read)
	c.Status.MasterPod = "c1-1"
	if err := h.r.writeStatus(ctx, &c, read); !errors.Is(err, errStaleStatus) {
		t.Errorf("writeStatus = %v, want errStaleStatus", err)
	}
	var stored kividbv1alpha1.KividbCluster
	if err := h.r.Get(ctx, key, &stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.MasterPod != "c1-2" {
		t.Errorf("stored masterPod %q, want the newer pass's c1-2", stored.Status.MasterPod)
	}
	if v, ok := stored.Annotations[kividbv1alpha1.AllowUnreadFailoverAnnotation]; ok {
		t.Errorf("%s is still %q after the failover it allowed", kividbv1alpha1.AllowUnreadFailoverAnnotation, v)
	}
}

// A conflict from a change outside the status (a label, an annotation) is
// settled by writing the same status onto the latest version.
func TestReplicatedStatusLandsAfterAConflictOutsideIt(t *testing.T) {
	f := steadyCase(t, nil)
	key := client.ObjectKeyFromObject(f.c)
	labeled := false
	f.r.Client = interceptor.NewClient(f.cl.(client.WithWatch), interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if _, ok := obj.(*kividbv1alpha1.KividbCluster); ok && !labeled {
				labeled = true
				var edited kividbv1alpha1.KividbCluster
				if err := cl.Get(ctx, key, &edited); err != nil {
					return err
				}
				edited.Labels = map[string]string{"team": "a"}
				if err := cl.Update(ctx, &edited); err != nil {
					return err
				}
			}
			return cl.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
	got := f.reconcile(t, reconcileInterval)
	if got.Labels["team"] != "a" || len(got.Status.Pods) != 2 {
		t.Fatalf("labels %v, %d pod statuses; want the label kept and this pass's status written", got.Labels, len(got.Status.Pods))
	}
}
