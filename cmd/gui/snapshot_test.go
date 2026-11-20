package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func replicatedSnapshotFixture(ref bool) *kividbv1alpha1.KividbCluster {
	c := &kividbv1alpha1.KividbCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "rep", Namespace: "prod"},
		Spec:       kividbv1alpha1.KividbClusterSpec{Replicas: 1},
	}
	if ref {
		c.Spec.SnapshotConfigRef = &corev1.LocalObjectReference{Name: "hourly"}
	}
	return c
}

func replicatedBackupCronJob() *batchv1.CronJob {
	labels := map[string]string{kividbv1alpha1.ClusterLabel: "rep", "app.kubernetes.io/name": "kividb-backup"}
	return &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{Name: "rep-backup", Namespace: "prod", UID: "cj-uid"},
		Spec: batchv1.CronJobSpec{JobTemplate: batchv1.JobTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: labels},
			Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "backup-trigger", Args: []string{"backup-trigger", "--targets", "x", "--role", "replica"}}},
			}}},
		}},
	}
}

// The master pod is there, with an address: the snapshot still goes
// through the CronJob, not straight to the master's agent, so the
// operator records it and the config's source and timeout apply.
func readyMasterPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "rep-0", Namespace: "prod", Labels: map[string]string{
			kividbv1alpha1.ClusterLabel: "rep", appNameLabel: appNameValue, "kividb.io/role": "master",
		}},
		Status: corev1.PodStatus{PodIP: "192.0.2.1"},
	}
}

func postSnapshot(s *server, name string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	req.SetPathValue("namespace", "prod")
	req.SetPathValue("name", name)
	rec := httptest.NewRecorder()
	s.handleAPISnapshot(rec, req)
	return rec
}

func backupJobs(t *testing.T, s *server) []batchv1.Job {
	t.Helper()
	jobs, err := s.clientset.BatchV1().Jobs("prod").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return jobs.Items
}

func TestReplicatedSnapshotStartsAJobFromTheCronJob(t *testing.T) {
	s := guiServer(t, []client.Object{replicatedSnapshotFixture(true)}, replicatedBackupCronJob(), readyMasterPod())
	rec := postSnapshot(s, "rep")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
	var out map[string]string
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	job, err := s.clientset.BatchV1().Jobs("prod").Get(context.Background(), out["job"], metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// The operator finds backup Jobs by the CronJob's labels.
	if !strings.HasPrefix(job.Name, "rep-backup-m") || job.Labels[kividbv1alpha1.ClusterLabel] != "rep" ||
		job.Labels["app.kubernetes.io/name"] != "kividb-backup" {
		t.Fatalf("job %s labels %v", job.Name, job.Labels)
	}
	if len(job.OwnerReferences) != 1 || job.OwnerReferences[0].UID != "cj-uid" {
		t.Fatalf("owner %+v", job.OwnerReferences)
	}
	if args := job.Spec.Template.Spec.Containers[0].Args; strings.Join(args, " ") != "backup-trigger --targets x --role replica" {
		t.Fatalf("args %v", args)
	}
}

func TestReplicatedSnapshotWithoutTheCronJobStartsNothing(t *testing.T) {
	s := guiServer(t, []client.Object{replicatedSnapshotFixture(true)}, readyMasterPod())
	rec := postSnapshot(s, "rep")
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "CronJob is not there yet") {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
	if jobs := backupJobs(t, s); len(jobs) != 0 {
		t.Fatalf("jobs %v", jobs)
	}
}

func TestSnapshotWithoutASnapshotConfigStartsNothing(t *testing.T) {
	s := guiServer(t, []client.Object{replicatedSnapshotFixture(false)}, replicatedBackupCronJob(), readyMasterPod())
	rec := postSnapshot(s, "rep")
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "snapshotConfigRef") {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
	if jobs := backupJobs(t, s); len(jobs) != 0 {
		t.Fatalf("jobs %v", jobs)
	}
}
