package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// withReason keeps the Events of reason from the ones events returns.
func withReason(all, reason string) []string {
	var out []string
	for _, e := range strings.Split(all, "\n") {
		if strings.HasPrefix(e, corev1.EventTypeWarning+" "+reason+" ") {
			out = append(out, e)
		}
	}
	return out
}

// wantWarnings checks that the Events since the last call are exactly the
// Warnings of reason that say msgs, in order.
func wantWarnings(t *testing.T, r *KividbClusterReconciler, reason string, msgs ...string) {
	t.Helper()
	got := withReason(events(r), reason)
	if len(got) != len(msgs) {
		t.Fatalf("%s Events %q, want %d saying %q", reason, got, len(msgs), msgs)
	}
	for i, msg := range msgs {
		if !strings.Contains(got[i], msg) {
			t.Errorf("%s Event %q, want it to say %q", reason, got[i], msg)
		}
	}
}

func cronJobOrService(which *string) func(client.Object) bool {
	return func(obj client.Object) bool {
		switch obj.(type) {
		case *batchv1.CronJob:
			return *which == "cronjob"
		case *corev1.Service:
			return *which == "services"
		}
		return false
	}
}

// A step the API server refuses is a Warning Event, not only a condition:
// kubectl describe is where most look first. Once per change: a refusal
// that lasts is not one Event every 5 s, which would also use up the
// object's Event budget that Failover needs. A new refusal is a new Event.
func TestReconcile_RefusedApplyStepWarnsOncePerChange(t *testing.T) {
	which := "cronjob"
	f := newFailoverCase(t, func(c *kividbv1alpha1.KividbCluster) {
		c.Spec.SnapshotConfigRef = &corev1.LocalObjectReference{Name: "snap"}
	}, cronJobOrService(&which), backupConfig())
	f.r.Recorder = record.NewFakeRecorder(100)

	f.wantFailedOver(t, f.reconcile(t, 5*time.Second), "ReconcileError", "reconciling backup cronjob")
	wantWarnings(t, f.r, "ReconcileError", "reconciling backup cronjob: c1-backup is invalid")

	f.reconcile(t, 5*time.Second)
	wantWarnings(t, f.r, "ReconcileError")

	// Services written by the first pass are not updated again: delete
	// one, so it is created, and refused.
	which = "services"
	if err := f.cl.Delete(context.Background(), &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: headlessServiceName(f.c)}}); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t, 5*time.Second)
	wantWarnings(t, f.r, "ReconcileError", "reconciling services: ")
}

// While a bootstrap waits, a refused step returns before the roles and
// the status: it is still a Warning Event, still once.
func TestReconcile_RefusedApplyStepWhileBootstrapWaitsWarnsOnce(t *testing.T) {
	which := "cronjob"
	f := bootstrapWaitingCase(t, &which)
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.c)}

	if _, err := f.r.Reconcile(ctx, req); err == nil || !strings.Contains(err.Error(), "reconciling backup cronjob") {
		t.Fatalf("Reconcile = %v, want the refused CronJob", err)
	}
	wantWarnings(t, f.r, "ReconcileError", "reconciling backup cronjob: c1-backup is invalid")
	var got kividbv1alpha1.KividbCluster
	if err := f.cl.Get(ctx, req.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	if ready := meta.FindStatusCondition(got.Status.Conditions, "Ready"); ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "ReconcileError" || !strings.Contains(ready.Message, "reconciling backup cronjob") {
		t.Errorf("Ready condition %+v, want False, ReconcileError, the refused CronJob", ready)
	}

	if _, err := f.r.Reconcile(ctx, req); err == nil {
		t.Fatal("second Reconcile: no error, want the refused CronJob")
	}
	wantWarnings(t, f.r, "ReconcileError")
}

// A reference that cannot be read holds back every write: a Warning
// Event too, once.
func TestReconcile_UnreadableReferenceWarnsOnce(t *testing.T) {
	f := newFailoverCase(t, func(c *kividbv1alpha1.KividbCluster) {
		c.Spec.ConfigRef = &corev1.LocalObjectReference{Name: "conf"}
	}, nil)
	f.r.Recorder = record.NewFakeRecorder(100)

	f.wantFailedOver(t, f.reconcile(t, 5*time.Second), "ReconcileError", `KividbConfig "conf"`)
	wantWarnings(t, f.r, "ReconcileError", `resolving configRef: KividbConfig "conf"`)
	f.reconcile(t, 5*time.Second)
	wantWarnings(t, f.r, "ReconcileError")
}

// InvalidSpec keeps its own once-per-change Warning, and is not also
// reported as ReconcileError.
func TestReconcile_InvalidReferenceWarnsOnce(t *testing.T) {
	f := newFailoverCase(t, func(c *kividbv1alpha1.KividbCluster) {
		c.Spec.SnapshotConfigRef = &corev1.LocalObjectReference{Name: "snap"}
	}, nil, &kividbv1alpha1.KividbSnapshotConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "snap", Namespace: "default"},
		Spec:       kividbv1alpha1.KividbSnapshotConfigSpec{Schedule: "0 2 * *"},
	})
	f.r.Recorder = record.NewFakeRecorder(100)

	f.reconcile(t, reconcileInterval)
	all := events(f.r)
	if got := withReason(all, "InvalidSpec"); len(got) != 1 || !strings.Contains(got[0], `spec.schedule "0 2 * *"`) {
		t.Errorf("InvalidSpec Events %q, want one naming the schedule", got)
	}
	if got := withReason(all, "ReconcileError"); len(got) != 0 {
		t.Errorf("ReconcileError Events %q, want none", got)
	}
	f.reconcile(t, reconcileInterval)
	wantWarnings(t, f.r, "InvalidSpec")
}

// The same for a sharded cluster: a refused step, once.
func TestReconcileSharded_RefusedApplyStepWarnsOnce(t *testing.T) {
	snap := &kividbv1alpha1.KividbSnapshotConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "hourly", Namespace: "default"},
		Spec: kividbv1alpha1.KividbSnapshotConfigSpec{Schedule: "0 * * * *",
			S3: kividbv1alpha1.S3StorageSpec{Endpoint: "https://s3.example.com", Bucket: "b"}},
	}
	h := formedForReconcile(t, func(obj client.Object) bool {
		_, ok := obj.(*batchv1.CronJob)
		return ok
	}, snap)
	h.c.Spec.SnapshotConfigRef = &corev1.LocalObjectReference{Name: "hourly"}
	if err := h.r.Update(context.Background(), h.c); err != nil {
		t.Fatal(err)
	}
	_ = events(h.r)

	reconcileFormed(t, h, "ReconcileError", "reconciling backup cronjob: s-backup is invalid")
	wantWarnings(t, h.r, "ReconcileError", "reconciling backup cronjob: s-backup is invalid")
	reconcileFormed(t, h, "ReconcileError", "reconciling backup cronjob: s-backup is invalid")
	wantWarnings(t, h.r, "ReconcileError")
}

// And a sharded cluster whose reference cannot be read: one Event, not
// one from reconcileHeld and another from reconcileSharded.
func TestReconcileSharded_UnreadableReferenceWarnsOnce(t *testing.T) {
	h := formedForReconcile(t, nil)
	h.c.Spec.ConfigRef = &corev1.LocalObjectReference{Name: "conf"}
	if err := h.r.Update(context.Background(), h.c); err != nil {
		t.Fatal(err)
	}
	_ = events(h.r)

	reconcileFormed(t, h, "ReconcileError", `resolving configRef: KividbConfig "conf"`)
	wantWarnings(t, h.r, "ReconcileError", `resolving configRef: KividbConfig "conf"`)
	reconcileFormed(t, h, "ReconcileError", `resolving configRef: KividbConfig "conf"`)
	wantWarnings(t, h.r, "ReconcileError")
}

// backupConfig is the KividbSnapshotConfig "snap" of a cluster with
// backups, valid as far as the operator can tell.
func backupConfig() *kividbv1alpha1.KividbSnapshotConfig {
	return &kividbv1alpha1.KividbSnapshotConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "snap", Namespace: "default"},
		Spec: kividbv1alpha1.KividbSnapshotConfigSpec{Schedule: "0 2 * * *",
			S3: kividbv1alpha1.S3StorageSpec{Endpoint: "s3.example", Bucket: "b", CredentialsSecretRef: kividbv1alpha1.S3CredentialsSecretRef{Name: "s3"}}},
	}
}

// bootstrapWaitingCase is newFailoverCase while a bootstrap waits (pods
// with the cluster's labels and no StatefulSet: an earlier cluster's), a
// backup CronJob refused while *which is "cronjob".
func bootstrapWaitingCase(t *testing.T, which *string) *failoverCase {
	t.Helper()
	f := newFailoverCase(t, func(c *kividbv1alpha1.KividbCluster) {
		c.Spec.SnapshotConfigRef = &corev1.LocalObjectReference{Name: "snap"}
		c.Spec.BootstrapFromSnapshot = &kividbv1alpha1.BootstrapFromSnapshotSpec{SnapshotRef: corev1.LocalObjectReference{Name: "seed"}}
	}, cronJobOrService(which), backupConfig())
	f.r.Recorder = record.NewFakeRecorder(100)
	return f
}

// While a bootstrap waits, a refusal that clears leaves no stale
// ReconcileError on Ready, and the same refusal coming back is a new
// Event.
func TestReconcile_BootstrapWaitClearsAFixedRefusal(t *testing.T) {
	which := "cronjob"
	f := bootstrapWaitingCase(t, &which)
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.c)}

	if _, err := f.r.Reconcile(ctx, req); err == nil {
		t.Fatal("Reconcile: no error, want the refused CronJob")
	}
	wantWarnings(t, f.r, "ReconcileError", "reconciling backup cronjob")

	which = ""
	if res, err := f.r.Reconcile(ctx, req); err != nil || res.RequeueAfter != 5*time.Second {
		t.Fatalf("Reconcile = %+v, %v; want the bootstrap's requeue in 5s", res, err)
	}
	if ready := f.ready(t); ready == nil || ready.Reason == "ReconcileError" || ready.Status != metav1.ConditionFalse {
		t.Errorf("Ready condition %+v after the CronJob was applied, want False and not ReconcileError", ready)
	}
	wantWarnings(t, f.r, "ReconcileError")

	// Created by the pass before, the CronJob is not updated again unless
	// changed: delete it, so it is created, and refused.
	which = "cronjob"
	if err := f.cl.Delete(ctx, &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: backupCronJobName(f.c)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.r.Reconcile(ctx, req); err == nil {
		t.Fatal("third Reconcile: no error, want the refused CronJob")
	}
	wantWarnings(t, f.r, "ReconcileError", "reconciling backup cronjob")
}

// The Ready write of a waiting bootstrap that loses a conflict is not an
// error of its own: the refused step is, and is retried anyway.
func TestReconcile_BootstrapWaitReadyConflictReturnsTheRefusal(t *testing.T) {
	which := "cronjob"
	f := bootstrapWaitingCase(t, &which)
	f.r.Client = interceptor.NewClient(f.r.Client.(client.WithWatch), interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if c, ok := obj.(*kividbv1alpha1.KividbCluster); ok {
				if ready := meta.FindStatusCondition(c.Status.Conditions, "Ready"); ready != nil && ready.Reason == "ReconcileError" {
					return apierrors.NewConflict(schema.GroupResource{Group: "kividb.io", Resource: "kividbclusters"}, c.Name, errors.New("modified"))
				}
			}
			return cl.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
	_, err := f.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.c)})
	if err == nil || !strings.Contains(err.Error(), "reconciling backup cronjob") || apierrors.IsConflict(err) || strings.Contains(err.Error(), "updating status") {
		t.Errorf("Reconcile = %v, want only the refused CronJob", err)
	}
}

// A conflict is a write that raced another, which the next pass retries:
// on the condition, but not a Warning Event, which would use up the
// object's Event budget Failover needs.
func TestReconcile_ConflictIsNotAWarning(t *testing.T) {
	f := newFailoverCase(t, func(c *kividbv1alpha1.KividbCluster) {
		c.Spec.SnapshotConfigRef = &corev1.LocalObjectReference{Name: "snap"}
	}, nil, backupConfig())
	f.r.Recorder = record.NewFakeRecorder(100)
	conflicts := 0
	f.r.Client = interceptor.NewClient(f.r.Client.(client.WithWatch), interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*batchv1.CronJob); ok && conflicts < 3 {
				conflicts++
				return apierrors.NewConflict(schema.GroupResource{Group: "batch", Resource: "cronjobs"}, obj.GetName(), errors.New("modified"))
			}
			return cl.Create(ctx, obj, opts...)
		},
	})
	for i := 0; i < 3; i++ {
		f.reconcile(t, 5*time.Second)
		if ready := f.ready(t); ready == nil || ready.Reason != "ReconcileError" || !strings.Contains(ready.Message, "reconciling backup cronjob") {
			t.Errorf("pass %d: Ready condition %+v, want the conflict on it", i, ready)
		}
	}
	wantWarnings(t, f.r, "ReconcileError")
}

// An error whose text starts the last one's is still a new error.
func TestWarnOnChangeComparesWholeErrors(t *testing.T) {
	for _, tc := range []struct {
		prev, err string
		want      int
	}{
		{"port: 70000", "port: 7000", 1},
		{"port: 7000", "port: 7000", 0},
		{"port: 7000\nlisting pods: timeout", "port: 7000", 0},
		{"port: 7000", "port: 70000", 1},
	} {
		r := &KividbClusterReconciler{Recorder: record.NewFakeRecorder(10)}
		c := &kividbv1alpha1.KividbCluster{Status: kividbv1alpha1.KividbClusterStatus{Conditions: []metav1.Condition{
			{Type: "Ready", Status: metav1.ConditionFalse, Reason: "ReconcileError", Message: tc.prev},
		}}}
		r.warnOnChange(c, "ReconcileError", errors.New(tc.err))
		if got := withReason(events(r), "ReconcileError"); len(got) != tc.want {
			t.Errorf("Ready says %q, error %q: Events %q, want %d", tc.prev, tc.err, got, tc.want)
		}
	}
}

// A waiting bootstrap's Ready condition is written only when it changes:
// a refusal that lasts adds no status write of its own to the pass.
func TestReconcile_BootstrapWaitWritesReadyOnlyOnChange(t *testing.T) {
	which := "cronjob"
	f := bootstrapWaitingCase(t, &which)
	writes := 0
	f.r.Client = interceptor.NewClient(f.r.Client.(client.WithWatch), interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			writes++
			return cl.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.c)}
	_, _ = f.r.Reconcile(context.Background(), req)
	first := writes
	writes = 0
	_, _ = f.r.Reconcile(context.Background(), req)
	// The bootstrap writes its own status every pass: one write fewer.
	if writes != first-1 {
		t.Errorf("status written %d times by the second pass, want %d (the first wrote %d, Ready among them)", writes, first-1, first)
	}
}
