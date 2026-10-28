package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// A restart waiting on a shard master that never hands over fails, with a
// message that says where to look, and withdraws its step-down request.
func TestDbOpsHandoverTimesOut(t *testing.T) {
	c := shardedCluster("s", 1, 1)
	zero := int32(0)
	c.Status.Shards = []kividbv1alpha1.ShardStatus{{Index: 0, MasterPod: "s-0-0"}}
	c.Status.Pods = []kividbv1alpha1.KividbPodStatus{
		{Name: "s-0-0", Shard: &zero, Role: kividbv1alpha1.RoleMaster, Ready: true, Synced: true},
		{Name: "s-0-1", Shard: &zero, Role: kividbv1alpha1.RoleReplica, Ready: true, Synced: true},
	}
	master := shardedPod(c, 0, 0, "10.0.0.1")
	replica := shardedPod(c, 0, 1, "10.0.0.2")
	op := &kividbv1alpha1.KividbDbOps{
		ObjectMeta: metav1.ObjectMeta{Name: "restart", Namespace: c.Namespace},
		Spec:       kividbv1alpha1.KividbDbOpsSpec{ClusterRef: corev1.LocalObjectReference{Name: c.Name}, Op: kividbv1alpha1.DbOpsRestart},
		Status: kividbv1alpha1.KividbDbOpsStatus{
			Phase:   kividbv1alpha1.DbOpsRunning,
			Restart: &kividbv1alpha1.DbOpsRestartStatus{PendingPods: []string{"s-0-0"}, CompletedPods: []string{"s-0-1"}},
		},
	}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := kividbv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	r := &KividbDbOpsReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(c, master, replica, op).
			WithStatusSubresource(&kividbv1alpha1.KividbCluster{}, &kividbv1alpha1.KividbDbOps{}).Build(),
		Scheme: scheme,
	}
	ctx := context.Background()
	// The status subresource drops what WithObjects was given; set it.
	if err := r.Status().Update(ctx, c); err != nil {
		t.Fatal(err)
	}
	if err := r.Status().Update(ctx, op); err != nil {
		t.Fatal(err)
	}
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(op)}
	get := func() *kividbv1alpha1.KividbDbOps {
		var got kividbv1alpha1.KividbDbOps
		if err := r.Get(ctx, req.NamespacedName, &got); err != nil {
			t.Fatal(err)
		}
		return &got
	}
	podAnnotation := func() string {
		var p corev1.Pod
		if err := r.Get(ctx, client.ObjectKeyFromObject(master), &p); err != nil {
			t.Fatal(err)
		}
		return p.Annotations[StepDownAnnotation]
	}

	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	got := get()
	if got.Status.Phase != kividbv1alpha1.DbOpsRunning || got.Status.Restart.HandoverStartTime == nil || podAnnotation() != "true" {
		t.Fatalf("first pass: phase %s, handover start %v, annotation %q", got.Status.Phase, got.Status.Restart.HandoverStartTime, podAnnotation())
	}

	// Still the master well past the timeout.
	past := metav1.NewTime(time.Now().Add(-dbOpsHandoverTimeout - time.Second))
	got.Status.Restart.HandoverStartTime = &past
	if err := r.Status().Update(ctx, got); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	got = get()
	if got.Status.Phase != kividbv1alpha1.DbOpsFailed {
		t.Fatalf("phase %s after the timeout, want Failed", got.Status.Phase)
	}
	for _, want := range []string{"s-0-0 did not hand over the master role within 5m0s", "ShardSwitchover", "Manual failover abandoned"} {
		if !strings.Contains(got.Status.Error, want) {
			t.Errorf("error %q lacks %q", got.Status.Error, want)
		}
	}
	if podAnnotation() != "" {
		t.Error("step-down request left on the master")
	}
}
