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
			AgentImage: "docker.io/kividb/kividb-operator-agent:test",
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

	t.Run("the Job asks every pod's agent, by its headless name, for the master", func(t *testing.T) {
		t.Parallel()
		snap := &kividbv1alpha1.KividbSnapshotConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "hourly"},
			Spec: kividbv1alpha1.KividbSnapshotConfigSpec{
				Schedule: "0 * * * *",
				// Source unset → master
			},
		}
		c := cluster.DeepCopy()
		c.Spec.Replicas = 2
		cj := desiredBackupCronJob(c, snap)
		container := cj.Spec.JobTemplate.Spec.Template.Spec.Containers[0]
		wantTargets := fmt.Sprintf("demo-0.demo-headless.prod.svc:%[1]d,demo-1.demo-headless.prod.svc:%[1]d,demo-2.demo-headless.prod.svc:%[1]d", AgentPort)
		if !containsArgPair(container.Args, "--targets", wantTargets) || !containsArgPair(container.Args, "--role", "master") {
			t.Fatalf("args %v, want --targets %s --role master", container.Args, wantTargets)
		}
		if len(container.Env) != 1 || container.Env[0].Name != "AGENT_TOKEN" ||
			container.Env[0].ValueFrom.SecretKeyRef.Name != "demo-auth" || container.Env[0].ValueFrom.SecretKeyRef.Key != "agent-token" {
			t.Fatalf("env %+v, want AGENT_TOKEN from demo-auth/agent-token", container.Env)
		}
	})

	t.Run("replica source asks for a replica", func(t *testing.T) {
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
		if !containsArgPair(args, "--role", "replica") {
			t.Fatalf("expected --role replica in args %v", args)
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
		if !strings.Contains(joined, "--role master") || strings.Contains(joined, "demo-master") {
			t.Fatalf("expected --role master, and no master Service, in args: %v", args)
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
