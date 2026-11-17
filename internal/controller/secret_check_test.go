package controller

import (
	"context"
	"testing"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
