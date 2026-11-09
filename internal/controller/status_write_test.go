package controller

import (
	"context"
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
func steadyCase(t *testing.T) *failoverCase {
	t.Helper()
	f := newFailoverCase(t, nil, nil)
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
	ctx := context.Background()
	f := steadyCase(t)
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
	if err != nil || res.RequeueAfter != 5*time.Second {
		t.Fatalf("Reconcile = %+v, %v; want no error, and a look again in 5s", res, err)
	}
	var got kividbv1alpha1.KividbCluster
	_ = f.cl.Get(ctx, key, &got)
	if got.ResourceVersion != newer.ResourceVersion {
		t.Fatalf("the stale pass wrote its status over the newer one (resourceVersion %s, want %s)", got.ResourceVersion, newer.ResourceVersion)
	}
}

// A conflict from a change outside the status (a label, an annotation) is
// settled by writing the same status onto the latest version.
func TestReplicatedStatusLandsAfterAConflictOutsideIt(t *testing.T) {
	f := steadyCase(t)
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
