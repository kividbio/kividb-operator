package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// tlsConfig is the KividbConfig "conf" with TLS on, its certificate in
// the Secret "tls".
func tlsConfig(ref kividbv1alpha1.TLSSecretRef) *kividbv1alpha1.KividbConfig {
	ref.Name = "tls"
	return &kividbv1alpha1.KividbConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "conf", Namespace: "default"},
		Spec:       kividbv1alpha1.KividbConfigSpec{TLS: &kividbv1alpha1.KividbTLSSpec{Enabled: true, Port: 6443, CertSecretRef: ref}},
	}
}

func secretWith(name string, keys ...string) *corev1.Secret {
	s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}, Data: map[string][]byte{}}
	for _, k := range keys {
		s.Data[k] = []byte("x")
	}
	return s
}

func withConfigRefs(c *kividbv1alpha1.KividbCluster) {
	c.Spec.ConfigRef = &corev1.LocalObjectReference{Name: "conf"}
	c.Spec.SnapshotConfigRef = &corev1.LocalObjectReference{Name: "snap"}
}

// A Secret the pods mount or read an env var from, missing or without a
// key they use, holds back every write: the kubelet would not start a pod
// made from that template. Failover goes on, and the Ready condition says
// which Secret and key.
func TestReconcile_MissingMountedSecretHoldsBackWrites(t *testing.T) {
	customS3 := backupConfig()
	customS3.Spec.S3.CredentialsSecretRef.AccessKeyIDKey = "AWS_ACCESS_KEY_ID"
	for _, tc := range []struct {
		name string
		objs []client.Object
		want string
	}{
		{"no TLS Secret", []client.Object{tlsConfig(kividbv1alpha1.TLSSecretRef{}), backupConfig(), s3Credentials("s3")},
			`KividbConfig conf spec.tls.certSecretRef: secret tls: secrets "tls" not found`},
		{"TLS Secret without its key", []client.Object{tlsConfig(kividbv1alpha1.TLSSecretRef{}), secretWith("tls", "tls.crt"), backupConfig(), s3Credentials("s3")},
			`KividbConfig conf spec.tls.certSecretRef: secret tls has no key "tls.key"`},
		{"TLS Secret without the caKey", []client.Object{tlsConfig(kividbv1alpha1.TLSSecretRef{CAKey: "ca.crt"}), secretWith("tls", "tls.crt", "tls.key"), backupConfig(), s3Credentials("s3")},
			`KividbConfig conf spec.tls.certSecretRef: secret tls has no key "ca.crt"`},
		{"TLS Secret without a renamed certKey", []client.Object{tlsConfig(kividbv1alpha1.TLSSecretRef{CertKey: "cert.pem"}), secretWith("tls", "tls.crt", "tls.key"), backupConfig(), s3Credentials("s3")},
			`KividbConfig conf spec.tls.certSecretRef: secret tls has no key "cert.pem"`},
		{"no S3 credentials Secret", []client.Object{tlsConfig(kividbv1alpha1.TLSSecretRef{}), secretWith("tls", "tls.crt", "tls.key"), backupConfig()},
			`KividbSnapshotConfig snap spec.s3.credentialsSecretRef: secret s3: secrets "s3" not found`},
		{"S3 Secret without a renamed key", []client.Object{tlsConfig(kividbv1alpha1.TLSSecretRef{}), secretWith("tls", "tls.crt", "tls.key"), customS3, s3Credentials("s3")},
			`KividbSnapshotConfig snap spec.s3.credentialsSecretRef: secret s3 has no key "AWS_ACCESS_KEY_ID"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFailoverCase(t, withConfigRefs, nil, tc.objs...)
			got := f.reconcile(t, 5*time.Second)
			f.wantFailedOver(t, got, "ReconcileError", "resolving referenced secrets: "+tc.want)
			f.wantNothingWritten(t)
		})
	}
}

// With every key there, nothing is held back: the StatefulSet mounts the
// TLS keys, CA included, and the agent reads the credentials' keys.
func TestReconcile_MountedSecretsWithTheirKeysAreApplied(t *testing.T) {
	f := newFailoverCase(t, withConfigRefs, nil,
		tlsConfig(kividbv1alpha1.TLSSecretRef{CAKey: "ca.crt"}), secretWith("tls", "tls.crt", "tls.key", "ca.crt"),
		backupConfig(), s3Credentials("s3"))
	f.reconcile(t, reconcileInterval)

	var sts appsv1.StatefulSet
	if err := f.cl.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: statefulSetName(f.c)}, &sts); err != nil {
		t.Fatalf("StatefulSet not applied: %v", err)
	}
	var items []string
	for _, v := range sts.Spec.Template.Spec.Volumes {
		if v.Secret != nil && v.Secret.SecretName == "tls" {
			for _, it := range v.Secret.Items {
				items = append(items, it.Key)
			}
		}
	}
	if len(items) != 3 {
		t.Errorf("TLS volume items %v, want tls.crt, tls.key and ca.crt", items)
	}
}

// The checked keys are the ones the pod template reads, defaults included.
func TestMountedSecretsNameTheKeysThePodsRead(t *testing.T) {
	tls := tlsConfig(kividbv1alpha1.TLSSecretRef{KeyKey: "key.pem"})
	snap := backupConfig()
	snap.Spec.S3.CredentialsSecretRef.SecretAccessKeyKey = "AWS_SECRET_ACCESS_KEY"
	got := mountedSecrets(tls, snap)
	if len(got) != 2 {
		t.Fatalf("mountedSecrets = %+v, want the TLS and S3 Secrets", got)
	}
	if want := []string{"tls.crt", "key.pem"}; got[0].name != "tls" || !equalStrings(got[0].keys, want) {
		t.Errorf("TLS Secret %s keys %v, want tls %v", got[0].name, got[0].keys, want)
	}
	if want := []string{"accessKeyId", "AWS_SECRET_ACCESS_KEY"}; got[1].name != "s3" || !equalStrings(got[1].keys, want) {
		t.Errorf("S3 Secret %s keys %v, want s3 %v", got[1].name, got[1].keys, want)
	}

	tls.Spec.TLS.Enabled = false
	if got := mountedSecrets(tls, nil); len(got) != 0 {
		t.Errorf("TLS off, no backups: mountedSecrets = %+v, want none", got)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// readyOf returns the Ready condition of the cluster key names.
func readyOf(t *testing.T, cl client.Client, key client.ObjectKey) *metav1.Condition {
	t.Helper()
	var got kividbv1alpha1.KividbCluster
	if err := cl.Get(context.Background(), key, &got); err != nil {
		t.Fatal(err)
	}
	return meta.FindStatusCondition(got.Status.Conditions, "Ready")
}

// A running cluster whose TLS Secret loses a key, or is deleted, is not
// rolled onto a template its pods could not start with, even by a change
// that would roll it; the Ready condition says why. Once the Secret is
// back, the held change goes out and the error is gone.
func TestReconcile_RunningClusterIsNotRolledOntoABrokenSecret(t *testing.T) {
	for _, tc := range []struct {
		name    string
		breakIt func(context.Context, client.Client) error
		want    string
	}{
		{"key removed", func(ctx context.Context, cl client.Client) error {
			return cl.Update(ctx, secretWith("tls", "tls.crt"))
		}, `secret tls has no key "tls.key"`},
		{"Secret deleted", func(ctx context.Context, cl client.Client) error {
			return cl.Delete(ctx, secretWith("tls"))
		}, `secret tls: secrets "tls" not found`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			conf := tlsConfig(kividbv1alpha1.TLSSecretRef{})
			f := newFailoverCase(t, withConfigRefs, nil, conf, secretWith("tls", "tls.crt", "tls.key"), backupConfig(), s3Credentials("s3"))
			f.reconcile(t, reconcileInterval)
			key := client.ObjectKey{Namespace: "default", Name: statefulSetName(f.c)}
			var before appsv1.StatefulSet
			if err := f.cl.Get(ctx, key, &before); err != nil {
				t.Fatalf("StatefulSet not applied: %v", err)
			}

			// A directive change rolls the pods; the Secret they would
			// mount is broken.
			if err := f.cl.Get(ctx, client.ObjectKeyFromObject(conf), conf); err != nil {
				t.Fatal(err)
			}
			conf.Spec.Directives = map[string]string{"loglevel": "verbose"}
			if err := f.cl.Update(ctx, conf); err != nil {
				t.Fatal(err)
			}
			if err := tc.breakIt(ctx, f.cl); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				f.reconcile(t, 5*time.Second)
			}

			var held appsv1.StatefulSet
			if err := f.cl.Get(ctx, key, &held); err != nil {
				t.Fatal(err)
			}
			if held.ResourceVersion != before.ResourceVersion || held.Generation != before.Generation ||
				!equality.Semantic.DeepEqual(held.Spec.Template, before.Spec.Template) {
				t.Errorf("StatefulSet updated while its TLS Secret is broken (generation %d -> %d)", before.Generation, held.Generation)
			}
			if ready := readyOf(t, f.cl, client.ObjectKeyFromObject(f.c)); ready == nil || ready.Reason != "ReconcileError" ||
				!strings.Contains(ready.Message, tc.want) {
				t.Errorf("Ready condition %+v, want ReconcileError saying %q", ready, tc.want)
			}

			fixed := secretWith("tls", "tls.crt", "tls.key")
			err := f.cl.Create(ctx, fixed)
			if apierrors.IsAlreadyExists(err) {
				err = f.cl.Update(ctx, fixed)
			}
			if err != nil {
				t.Fatal(err)
			}
			f.reconcile(t, reconcileInterval)
			var after appsv1.StatefulSet
			if err := f.cl.Get(ctx, key, &after); err != nil {
				t.Fatal(err)
			}
			if after.Spec.Template.Annotations[ConfigHashAnnotation] == before.Spec.Template.Annotations[ConfigHashAnnotation] {
				t.Error("the held directive change was not applied once the Secret was fixed")
			}
			if ready := readyOf(t, f.cl, client.ObjectKeyFromObject(f.c)); ready != nil && ready.Reason == "ReconcileError" {
				t.Errorf("Ready condition %+v, want the error gone", ready)
			}
		})
	}
}

// A sharded cluster is held back the same way: its shards' StatefulSets
// are not given an S3 env their agents could not start with, and are
// once the Secret is made.
func TestReconcileSharded_MissingS3SecretHoldsBackUntilMade(t *testing.T) {
	ctx := context.Background()
	snap := backupConfig()
	h := formedForReconcile(t, nil, snap)
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(h.c)}
	// A running cluster: its shards' StatefulSets are written, without
	// backups.
	if _, err := h.r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	_ = h.r.Get(ctx, req.NamespacedName, h.c)
	h.c.Spec.SnapshotConfigRef = &corev1.LocalObjectReference{Name: snap.Name}
	if err := h.r.Update(ctx, h.c); err != nil {
		t.Fatal(err)
	}
	s3Env := func(shard int32) bool {
		var sts appsv1.StatefulSet
		if err := h.r.Get(ctx, client.ObjectKey{Namespace: "default", Name: shardStatefulSetName(h.c, shard)}, &sts); err != nil {
			t.Fatal(err)
		}
		for _, ct := range sts.Spec.Template.Spec.Containers {
			for _, e := range ct.Env {
				if e.Name == "S3_ACCESS_KEY_ID" {
					return true
				}
			}
		}
		return false
	}

	reconcileFormed(t, h, "ReconcileError",
		`resolving referenced secrets: KividbSnapshotConfig snap spec.s3.credentialsSecretRef: secret s3: secrets "s3" not found`)
	for s := int32(0); s < 2; s++ {
		if s3Env(s) {
			t.Errorf("shard %d StatefulSet given the S3 env while its Secret is missing", s)
		}
	}

	if err := h.r.Create(ctx, s3Credentials("s3")); err != nil {
		t.Fatal(err)
	}
	if _, err := h.r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	for s := int32(0); s < 2; s++ {
		if !s3Env(s) {
			t.Errorf("shard %d StatefulSet has no S3 env once the Secret is there", s)
		}
	}
	if ready := readyOf(t, h.r.Client, client.ObjectKeyFromObject(h.c)); ready != nil && ready.Reason == "ReconcileError" {
		t.Errorf("Ready condition %+v, want the error gone", ready)
	}
}

// The restore Job reads its snapshot's S3 credentials: it is not created
// while their Secret or a key is missing, which status.bootstrap.error and
// the Ready condition say, and the Secret is watched for the cluster;
// once it is fixed, the restore starts.
func TestBootstrapWaitsForItsS3Secret(t *testing.T) {
	for _, tc := range []struct {
		name   string
		broken *corev1.Secret // nil: no Secret
		want   string
	}{
		{"Secret missing", nil,
			`KividbSnapshotConfig s3 spec.s3.credentialsSecretRef: secret s3: secrets "s3" not found`},
		{"key missing", secretWith("s3", "accessKeyId"),
			`KividbSnapshotConfig s3 spec.s3.credentialsSecretRef: secret s3 has no key "secretAccessKey"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			c, objs := bootstrapFixture("fresh")
			objs = objs[:len(objs)-1] // the fixture's s3Credentials
			if tc.broken != nil {
				objs = append(objs, tc.broken)
			}
			r := shardedReconciler(t, append(objs, c)...)
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(c)}
			jobs := func() int {
				var list batchv1.JobList
				if err := r.List(ctx, &list); err != nil {
					t.Fatal(err)
				}
				return len(list.Items)
			}

			for i := 0; i < 2; i++ {
				if res, err := r.Reconcile(ctx, req); err != nil || res.RequeueAfter != 5*time.Second {
					t.Fatalf("pass %d: Reconcile = %+v, %v; want a requeue in 5s and no error", i, res, err)
				}
			}
			var got kividbv1alpha1.KividbCluster
			if err := r.Get(ctx, req.NamespacedName, &got); err != nil {
				t.Fatal(err)
			}
			if b := got.Status.Bootstrap; b == nil || b.Phase != kividbv1alpha1.BootstrapPending || !strings.Contains(b.Error, tc.want) {
				t.Errorf("bootstrap status %+v; want Pending, with the error %q", b, tc.want)
			}
			if ready := meta.FindStatusCondition(got.Status.Conditions, "Ready"); ready == nil || ready.Reason != "ReconcileError" || !strings.Contains(ready.Message, tc.want) {
				t.Errorf("Ready condition %+v; want ReconcileError saying %q", ready, tc.want)
			}
			if n := jobs(); n != 0 {
				t.Errorf("%d restore Jobs created without their S3 credentials", n)
			}
			if got := withReason(events(r), "ReconcileError"); len(got) != 1 {
				t.Errorf("ReconcileError Events %q, want one", got)
			}
			if reqs := r.mapReferenceToClusters(ctx, s3Credentials("s3")); len(reqs) != 1 || reqs[0] != req {
				t.Errorf("the S3 Secret enqueues %v, want %v", reqs, req)
			}

			fixed := s3Credentials("s3")
			err := r.Create(ctx, fixed)
			if apierrors.IsAlreadyExists(err) {
				err = r.Update(ctx, fixed)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := r.Reconcile(ctx, req); err != nil {
				t.Fatal(err)
			}
			if err := r.Get(ctx, req.NamespacedName, &got); err != nil {
				t.Fatal(err)
			}
			if b := got.Status.Bootstrap; b == nil || b.Phase != kividbv1alpha1.BootstrapInProgress || b.Error != "" {
				t.Errorf("bootstrap status %+v; want the restore in progress, the error gone", b)
			}
			if n := jobs(); n != 1 {
				t.Errorf("%d restore Jobs, want one once the Secret is fixed", n)
			}
		})
	}
}

// A restore Job already made read its credentials when its pod started:
// the S3 Secret deleted while it runs neither sends the bootstrap back to
// Pending nor keeps its success from being recorded.
func TestBootstrapUnderWayIsLeftAloneWhenItsS3SecretGoes(t *testing.T) {
	ctx := context.Background()
	c, objs := bootstrapFixture("fresh")
	r := shardedReconciler(t, append(objs, c)...)
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(c)}
	status := func() *kividbv1alpha1.BootstrapStatus {
		var got kividbv1alpha1.KividbCluster
		if err := r.Get(ctx, req.NamespacedName, &got); err != nil {
			t.Fatal(err)
		}
		return got.Status.Bootstrap
	}
	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatal(err)
		}
	}
	if b := status(); b == nil || b.Phase != kividbv1alpha1.BootstrapInProgress {
		t.Fatalf("bootstrap status %+v; want the restore in progress", b)
	}

	if err := r.Delete(ctx, s3Credentials("s3")); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs); err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 1 {
		t.Fatalf("%d restore Jobs, want the one under way", len(jobs.Items))
	}
	if b := status(); b == nil || b.Phase != kividbv1alpha1.BootstrapInProgress || b.Error != "" {
		t.Errorf("bootstrap status %+v; want it still in progress, no error", b)
	}

	job := jobs.Items[0]
	job.Status.Succeeded = 1
	if err := r.Status().Update(ctx, &job); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if b := status(); b == nil || !b.Completed || b.Phase != kividbv1alpha1.BootstrapCompleted {
		t.Errorf("bootstrap status %+v; want the restore recorded as done", b)
	}
	var sts appsv1.StatefulSet
	if err := r.Get(ctx, req.NamespacedName, &sts); err != nil {
		t.Fatal(err)
	}
	if *sts.Spec.Replicas == 0 {
		t.Error("StatefulSet held at 0 replicas after the restore was done")
	}
}
