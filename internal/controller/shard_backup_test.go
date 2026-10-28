package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	"github.com/kividbio/kividb-operator/internal/agentapi"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestShardedBackupCronJob(t *testing.T) {
	c := shardedCluster("cache", 2, 1)
	c.Namespace = "prod"
	snap := &kividbv1alpha1.KividbSnapshotConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "hourly"},
		Spec:       kividbv1alpha1.KividbSnapshotConfigSpec{Schedule: "0 * * * *", Source: kividbv1alpha1.SnapshotSourceReplica},
	}
	args := desiredBackupCronJob(c, snap).Spec.JobTemplate.Spec.Template.Spec.Containers[0].Args
	want := fmt.Sprintf("cache-0-0.cache-0-headless.prod.svc:%[1]d,cache-0-1.cache-0-headless.prod.svc:%[1]d;"+
		"cache-1-0.cache-1-headless.prod.svc:%[1]d,cache-1-1.cache-1-headless.prod.svc:%[1]d", AgentPort)
	if !containsArgPair(args, "--shards", want) || !containsArgPair(args, "--cluster", "cache") || !containsArgPair(args, "--role", "replica") {
		t.Fatalf("args %v", args)
	}
	for _, a := range args {
		if a == "--targets" {
			t.Fatalf("--targets in a sharded cluster's backup: %v", args)
		}
	}
}

func TestReconcileShardedBackups(t *testing.T) {
	c := shardedCluster("s", 2, 0)
	snap := &kividbv1alpha1.KividbSnapshotConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "hourly", Namespace: "default"},
		Spec: kividbv1alpha1.KividbSnapshotConfigSpec{
			Schedule: "0 * * * *", Retention: 5,
			S3: kividbv1alpha1.S3StorageSpec{Endpoint: "https://s3.example.com", Bucket: "b"},
		},
	}
	r := shardedReconciler(t, c)
	ctx := context.Background()
	var got kividbv1alpha1.KividbCluster
	_ = r.Get(ctx, client.ObjectKeyFromObject(c), &got)
	if _, err := r.reconcileSharded(ctx, &got, nil, nil, snap, "", "", authGenerations{auth: "1"}); err != nil {
		t.Fatal(err)
	}
	var cj batchv1.CronJob
	if err := r.Get(ctx, client.ObjectKey{Namespace: "default", Name: backupCronJobName(c)}, &cj); err != nil {
		t.Fatalf("no backup CronJob: %v", err)
	}
	// Every shard's pods carry the S3 settings their agent uploads with.
	for s := int32(0); s < 2; s++ {
		var sts appsv1.StatefulSet
		_ = r.Get(ctx, client.ObjectKey{Namespace: "default", Name: shardStatefulSetName(c, s)}, &sts)
		env := map[string]string{}
		for _, ct := range sts.Spec.Template.Spec.Containers {
			for _, e := range ct.Env {
				env[e.Name] = e.Value
			}
		}
		if env["S3_BUCKET"] != "b" || env["S3_RETENTION"] != "5" {
			t.Fatalf("shard %d agent env %v", s, env)
		}
	}
}

func TestSnapshotStatusOfAShardedBackup(t *testing.T) {
	c := shardedCluster("s", 2, 0)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "s-backup-1", Namespace: "default"},
		Status:     batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}},
	}
	msg := `{"success":true,"objectKey":"s/20261025T101500Z/manifest.json","sizeBytes":201,"sourceRole":"master",` +
		`"shards":[{"i":0,"pod":"s-0-0","size":100},{"i":1,"pod":"s-1-0","size":101}]}`
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "s-backup-1-x", Namespace: "default", Labels: map[string]string{"job-name": job.Name}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			Name: "backup-trigger", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Message: msg}},
		}}},
	}
	r := shardedReconciler(t, c, pod)
	var snap kividbv1alpha1.KividbSnapshot
	populateSnapshotStatus(context.Background(), r.Client, job, &snap)
	st := snap.Status
	if st.Phase != kividbv1alpha1.SnapshotSucceeded || !strings.HasSuffix(st.ObjectKey, "/"+agentapi.ManifestName) || st.SizeBytes != 201 {
		t.Fatalf("status %+v", st)
	}
	if len(st.Shards) != 2 || st.Shards[1].SourcePod != "s-1-0" || st.Shards[1].SizeBytes != 101 {
		t.Fatalf("shards %+v", st.Shards)
	}
	if cp := st.DeepCopy(); &cp.Shards[0] == &st.Shards[0] {
		t.Fatal("DeepCopy shares the shards")
	}
}
