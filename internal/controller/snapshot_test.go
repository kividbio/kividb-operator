package controller

import (
	"context"
	"testing"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// snapshotReconciler is a reconciler whose fake API server has the
// KividbSnapshot status subresource, as the real one does.
func snapshotReconciler(t *testing.T, objs ...client.Object) *KividbClusterReconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := kividbv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
		WithStatusSubresource(&kividbv1alpha1.KividbCluster{}, &kividbv1alpha1.KividbSnapshot{}).Build()
	return &KividbClusterReconciler{Client: cl, Scheme: scheme}
}

func snapshotCluster() *kividbv1alpha1.KividbCluster {
	return &kividbv1alpha1.KividbCluster{ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "default"}}
}

// backupJob is a backup Job of c started for the KividbSnapshotConfig
// config ("" for a Job without the label).
func backupJob(c *kividbv1alpha1.KividbCluster, name, config string, conds ...batchv1.JobConditionType) *batchv1.Job {
	labels := backupLabels(c)
	if config != "" {
		labels[snapshotConfigLabel] = config
	}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: c.Namespace, Labels: labels}}
	for _, ct := range conds {
		job.Status.Conditions = append(job.Status.Conditions, batchv1.JobCondition{
			Type: ct, Status: corev1.ConditionTrue, Message: "BackoffLimitExceeded",
		})
	}
	return job
}

// backupPod is a pod of job whose backup-trigger container wrote msg.
func backupPod(job *batchv1.Job, name, msg string, started time.Time) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: job.Namespace, Labels: map[string]string{"job-name": job.Name},
			CreationTimestamp: metav1.NewTime(started),
		},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			Name: "backup-trigger", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Message: msg}},
		}}},
	}
}

func getSnapshot(t *testing.T, r *KividbClusterReconciler, name string) *kividbv1alpha1.KividbSnapshot {
	t.Helper()
	return getSnapshotFrom(t, r.Client, name)
}

// After spec.snapshotConfigRef moves to another config, the CronJob keeps
// the earlier config's Jobs in its history. Their records must still name
// the config, and so the bucket, their backups went to: bootstrap reads
// spec.snapshotConfigRef to find the object.
func TestSnapshotRecordKeepsTheConfigOfItsJob(t *testing.T) {
	c := snapshotCluster()
	old := backupJob(c, "c1-backup-1", "old", batchv1.JobComplete)
	unlabelled := backupJob(c, "c1-backup-2", "", batchv1.JobComplete)
	r := snapshotReconciler(t, c, old, unlabelled)
	ctx := context.Background()

	if err := r.reconcileSnapshots(ctx, c, "old"); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcileSnapshots(ctx, c, "new"); err != nil {
		t.Fatal(err)
	}

	snap := getSnapshot(t, r, old.Name)
	if snap.Spec.SnapshotConfigRef.Name != "old" || snap.Labels[snapshotConfigLabel] != "old" {
		t.Errorf("record of a Job of config old: spec.snapshotConfigRef %q, label %q; want both old",
			snap.Spec.SnapshotConfigRef.Name, snap.Labels[snapshotConfigLabel])
	}
	snap = getSnapshot(t, r, unlabelled.Name)
	if snap.Spec.SnapshotConfigRef.Name != "new" || snap.Labels[snapshotConfigLabel] != "new" {
		t.Errorf("record of a Job without the label: spec.snapshotConfigRef %q, label %q; want the cluster's, new",
			snap.Spec.SnapshotConfigRef.Name, snap.Labels[snapshotConfigLabel])
	}
}

// A record written before this fix names the cluster's config of the
// time; while its Job is there, the next pass puts the Job's back.
func TestSnapshotRecordOfAnEarlierConfigIsCorrected(t *testing.T) {
	c := snapshotCluster()
	job := backupJob(c, "c1-backup-1", "old", batchv1.JobComplete)
	stale := &kividbv1alpha1.KividbSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: job.Name, Namespace: c.Namespace, Labels: map[string]string{
			kividbv1alpha1.ClusterLabel: c.Name, snapshotConfigLabel: "new",
		}},
		Spec: kividbv1alpha1.KividbSnapshotSpec{
			ClusterRef:        corev1.LocalObjectReference{Name: c.Name},
			SnapshotConfigRef: corev1.LocalObjectReference{Name: "new"},
		},
	}
	r := snapshotReconciler(t, c, job, stale)

	if err := r.reconcileSnapshots(context.Background(), c, "new"); err != nil {
		t.Fatal(err)
	}
	if got := getSnapshot(t, r, job.Name); got.Spec.SnapshotConfigRef.Name != "old" || got.Labels[snapshotConfigLabel] != "old" {
		t.Errorf("spec.snapshotConfigRef %q, label %q; want both old", got.Spec.SnapshotConfigRef.Name, got.Labels[snapshotConfigLabel])
	}
}

// A backup Job retries a failed pod. Once a retry has succeeded, the
// record shows Succeeded with no error, not the failed attempt's.
func TestSnapshotOfARetriedBackupShowsOnlyItsLastError(t *testing.T) {
	c := snapshotCluster()
	job := backupJob(c, "c1-backup-1", "snap")
	first := backupPod(job, "c1-backup-1-a", `{"success":false,"error":"no agent reports role master"}`, time.Now().Add(-time.Minute))
	r := snapshotReconciler(t, c, job, first)
	ctx := context.Background()

	if err := r.reconcileSnapshots(ctx, c, "snap"); err != nil {
		t.Fatal(err)
	}
	if got := getSnapshot(t, r, job.Name).Status; got.Phase != kividbv1alpha1.SnapshotInProgress || got.Error != "no agent reports role master" {
		t.Fatalf("after the first attempt: phase %q, error %q; want InProgress with its error", got.Phase, got.Error)
	}

	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if err := r.Status().Update(ctx, job); err != nil {
		t.Fatal(err)
	}
	second := backupPod(job, "c1-backup-1-b", `{"success":true,"objectKey":"default/c1/20261117T020000Z.kdb","sizeBytes":42}`, time.Now())
	if err := r.Create(ctx, second); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcileSnapshots(ctx, c, "snap"); err != nil {
		t.Fatal(err)
	}
	got := getSnapshot(t, r, job.Name).Status
	if got.Phase != kividbv1alpha1.SnapshotSucceeded || got.Error != "" || got.ObjectKey != "default/c1/20261117T020000Z.kdb" {
		t.Errorf("after the retry succeeded: phase %q, error %q, object key %q; want Succeeded, no error, the retry's key",
			got.Phase, got.Error, got.ObjectKey)
	}
}

// A Job that failed outright keeps its error.
func TestSnapshotOfAFailedBackupKeepsTheJobsError(t *testing.T) {
	c := snapshotCluster()
	job := backupJob(c, "c1-backup-1", "snap", batchv1.JobFailed)
	r := snapshotReconciler(t, c, job)

	if err := r.reconcileSnapshots(context.Background(), c, "snap"); err != nil {
		t.Fatal(err)
	}
	if got := getSnapshot(t, r, job.Name).Status; got.Phase != kividbv1alpha1.SnapshotFailed || got.Error != "BackoffLimitExceeded" {
		t.Errorf("phase %q, error %q; want Failed, BackoffLimitExceeded", got.Phase, got.Error)
	}
}

// A record whose Job is gone before it finished is failed; finished
// records, and other clusters' records, are left as they are.
func TestSnapshotWhoseJobIsGoneIsFailed(t *testing.T) {
	c := snapshotCluster()
	record := func(name, cluster string, phase kividbv1alpha1.SnapshotPhase) *kividbv1alpha1.KividbSnapshot {
		return &kividbv1alpha1.KividbSnapshot{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: c.Namespace, Labels: map[string]string{kividbv1alpha1.ClusterLabel: cluster}},
			Status:     kividbv1alpha1.KividbSnapshotStatus{Phase: phase},
		}
	}
	live := backupJob(c, "c1-backup-live", "snap")
	r := snapshotReconciler(t, c, live,
		record("c1-backup-gone", c.Name, kividbv1alpha1.SnapshotInProgress),
		record("c1-backup-unwritten", c.Name, ""),
		record("c1-backup-done", c.Name, kividbv1alpha1.SnapshotSucceeded),
		record("other-backup-gone", "other", kividbv1alpha1.SnapshotInProgress),
	)
	ctx := context.Background()
	// The status subresource drops what WithObjects was given; set it.
	for _, name := range []string{"c1-backup-gone", "c1-backup-done", "other-backup-gone"} {
		snap := getSnapshot(t, r, name)
		snap.Status.Phase = kividbv1alpha1.SnapshotInProgress
		if name == "c1-backup-done" {
			snap.Status.Phase = kividbv1alpha1.SnapshotSucceeded
		}
		if err := r.Status().Update(ctx, snap); err != nil {
			t.Fatal(err)
		}
	}

	if err := r.reconcileSnapshots(ctx, c, "snap"); err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{"c1-backup-gone", "c1-backup-unwritten"} {
		got := getSnapshot(t, r, name).Status
		if got.Phase != kividbv1alpha1.SnapshotFailed || got.CompletionTime == nil ||
			got.Error != "backup Job "+name+" was deleted before it finished" {
			t.Errorf("%s: phase %q, completion %v, error %q; want Failed, a completion time, the Job named", name, got.Phase, got.CompletionTime, got.Error)
		}
	}
	if got := getSnapshot(t, r, "c1-backup-done").Status; got.Phase != kividbv1alpha1.SnapshotSucceeded || got.Error != "" {
		t.Errorf("finished record: phase %q, error %q; want it left Succeeded", got.Phase, got.Error)
	}
	if got := getSnapshot(t, r, "other-backup-gone").Status; got.Phase != kividbv1alpha1.SnapshotInProgress {
		t.Errorf("another cluster's record: phase %q; want it left InProgress", got.Phase)
	}
	if got := getSnapshot(t, r, live.Name).Status; got.Phase != kividbv1alpha1.SnapshotInProgress {
		t.Errorf("record of a running Job: phase %q; want InProgress", got.Phase)
	}
}

// Removing spec.snapshotConfigRef deletes the CronJob, and its Jobs with
// it; a backup running then is failed by the next reconcile, which still
// looks at the records with no config set.
func TestReconcileFailsARunningSnapshotOnceTheConfigIsRemoved(t *testing.T) {
	inFlight := &kividbv1alpha1.KividbSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "c1-backup-1", Namespace: "default", Labels: map[string]string{kividbv1alpha1.ClusterLabel: "c1"}},
	}
	f := newFailoverCase(t, nil, nil, inFlight)
	ctx := context.Background()
	snap := getSnapshotFrom(t, f.cl, inFlight.Name)
	snap.Status.Phase = kividbv1alpha1.SnapshotInProgress
	if err := f.cl.Status().Update(ctx, snap); err != nil {
		t.Fatal(err)
	}

	f.reconcile(t, reconcileInterval)

	if got := getSnapshotFrom(t, f.cl, inFlight.Name).Status; got.Phase != kividbv1alpha1.SnapshotFailed {
		t.Errorf("phase %q, error %q; want Failed", got.Phase, got.Error)
	}
}

func getSnapshotFrom(t *testing.T, cl client.Client, name string) *kividbv1alpha1.KividbSnapshot {
	t.Helper()
	var snap kividbv1alpha1.KividbSnapshot
	if err := cl.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: name}, &snap); err != nil {
		t.Fatal(err)
	}
	return &snap
}
