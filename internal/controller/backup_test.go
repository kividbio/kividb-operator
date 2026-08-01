package controller

import (
	"fmt"
	"strings"
	"testing"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestDesiredBackupCronJob(t *testing.T) {
	t.Parallel()

	cluster := &kividbv1alpha1.KividbCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "prod"},
		Spec: kividbv1alpha1.KividbClusterSpec{
			AgentImage: "quay.io/kividbio/kividb-operator-agent:test",
		},
	}

	t.Run("ForbidConcurrent", func(t *testing.T) {
		t.Parallel()
		snap := &kividbv1alpha1.KividbSnapshotConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "hourly"},
			Spec: kividbv1alpha1.KividbSnapshotConfigSpec{
				Schedule: "0 * * * *",
			},
		}
		cj := desiredBackupCronJob(cluster, snap)
		if cj.Spec.ConcurrencyPolicy != batchv1.ForbidConcurrent {
			t.Fatalf("ConcurrencyPolicy = %q, want ForbidConcurrent", cj.Spec.ConcurrencyPolicy)
		}
	})

	t.Run("backup URL points at master service by default", func(t *testing.T) {
		t.Parallel()
		snap := &kividbv1alpha1.KividbSnapshotConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "hourly"},
			Spec: kividbv1alpha1.KividbSnapshotConfigSpec{
				Schedule: "0 * * * *",
				// Source unset → master
			},
		}
		cj := desiredBackupCronJob(cluster, snap)
		args := cj.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Args
		wantURL := fmt.Sprintf("http://demo-master.prod.svc:%d/backup", AgentPort)
		if !containsArgPair(args, "--url", wantURL) {
			t.Fatalf("expected --url %s in args %v", wantURL, args)
		}
	})

	t.Run("replica source uses replicas service", func(t *testing.T) {
		t.Parallel()
		snap := &kividbv1alpha1.KividbSnapshotConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "hourly"},
			Spec: kividbv1alpha1.KividbSnapshotConfigSpec{
				Schedule: "0 * * * *",
				Source:   kividbv1alpha1.SnapshotSourceReplica,
			},
		}
		cj := desiredBackupCronJob(cluster, snap)
		args := cj.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Args
		wantURL := fmt.Sprintf("http://demo-replicas.prod.svc:%d/backup", AgentPort)
		if !containsArgPair(args, "--url", wantURL) {
			t.Fatalf("expected --url %s in args %v", wantURL, args)
		}
	})

	t.Run("timeout from SnapshotConfig", func(t *testing.T) {
		t.Parallel()
		timeout := int32(120)
		snap := &kividbv1alpha1.KividbSnapshotConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "hourly"},
			Spec: kividbv1alpha1.KividbSnapshotConfigSpec{
				Schedule:       "0 * * * *",
				TimeoutSeconds: &timeout,
			},
		}
		cj := desiredBackupCronJob(cluster, snap)
		args := cj.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Args
		if !containsArgPair(args, "--timeout", "120s") {
			t.Fatalf("expected --timeout 120s in args %v", args)
		}
		if cj.Spec.JobTemplate.Spec.ActiveDeadlineSeconds == nil {
			t.Fatal("ActiveDeadlineSeconds should be set")
		}
		wantDeadline := int64(timeout + 60)
		if *cj.Spec.JobTemplate.Spec.ActiveDeadlineSeconds != wantDeadline {
			t.Fatalf("ActiveDeadlineSeconds = %d, want %d", *cj.Spec.JobTemplate.Spec.ActiveDeadlineSeconds, wantDeadline)
		}
	})

	t.Run("default timeout when unset", func(t *testing.T) {
		t.Parallel()
		snap := &kividbv1alpha1.KividbSnapshotConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "hourly"},
			Spec:       kividbv1alpha1.KividbSnapshotConfigSpec{Schedule: "0 * * * *"},
		}
		cj := desiredBackupCronJob(cluster, snap)
		args := cj.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Args
		if !containsArgPair(args, "--timeout", "900s") {
			t.Fatalf("expected default --timeout 900s in args %v", args)
		}
	})

	t.Run("explicit master source", func(t *testing.T) {
		t.Parallel()
		snap := &kividbv1alpha1.KividbSnapshotConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "hourly"},
			Spec: kividbv1alpha1.KividbSnapshotConfigSpec{
				Schedule: "0 * * * *",
				Source:   kividbv1alpha1.SnapshotSourceMaster,
			},
		}
		cj := desiredBackupCronJob(cluster, snap)
		args := cj.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Args
		joined := strings.Join(args, " ")
		if !strings.Contains(joined, "demo-master.prod.svc") {
			t.Fatalf("expected master service in args: %v", args)
		}
	})
}

func containsArgPair(args []string, flag, value string) bool {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == flag && args[i+1] == value {
			return true
		}
	}
	return false
}
