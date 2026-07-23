package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	"github.com/kividbio/kividb-operator/internal/agentapi"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// snapshotSourceServiceName picks which Service (master or replica) a
// backup CronJob's backup-trigger client targets, per the owning
// KividbSnapshotConfig's spec.source. Defaults to master (also the
// recommended, and CRD-defaulted, value) for any unrecognized value.
func snapshotSourceServiceName(c *kividbv1alpha1.KividbCluster, source kividbv1alpha1.SnapshotSource) string {
	if source == kividbv1alpha1.SnapshotSourceReplica {
		return replicaServiceName(c)
	}
	return masterServiceName(c)
}

// desiredBackupCronJob builds the CronJob that triggers scheduled backups,
// from the KividbCluster's referenced KividbSnapshotConfig. The Job's
// single container is the *same agent image* running in "backup-trigger"
// client mode: it makes one HTTP POST to the target Service (which always
// resolves to whichever pod currently holds the configured role,
// failover-safe by construction) at /backup, waits for the response, and
// exits 0/non-zero accordingly. All S3 upload logic and credentials stay
// inside the target pod's own agent sidecar -- the CronJob pod never
// mounts the data PVC and never sees S3 credentials.
func desiredBackupCronJob(c *kividbv1alpha1.KividbCluster, snapCfg *kividbv1alpha1.KividbSnapshotConfig) *batchv1.CronJob {
	timeout := int32(900)
	if snapCfg.Spec.TimeoutSeconds != nil {
		timeout = *snapCfg.Spec.TimeoutSeconds
	}

	source := snapCfg.Spec.Source
	if source == "" {
		source = kividbv1alpha1.SnapshotSourceMaster
	}
	backupURL := fmt.Sprintf("http://%s.%s.svc:%d/backup", snapshotSourceServiceName(c, source), c.Namespace, AgentPort)

	resources := snapCfg.Spec.JobResources
	if resources.Requests == nil && resources.Limits == nil {
		resources = corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("50m"),
				corev1.ResourceMemory: resource.MustParse("64Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("250m"),
				corev1.ResourceMemory: resource.MustParse("256Mi"),
			},
		}
	}

	backoffLimit := int32(2)
	activeDeadline := timeout + 60
	successHistory := int32(3)
	failedHistory := int32(3)
	labels := backupLabels(c)
	labels["kividb.io/snapshot-config"] = snapCfg.Name

	return &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      backupCronJobName(c),
			Namespace: c.Namespace,
			Labels:    labels,
		},
		Spec: batchv1.CronJobSpec{
			Schedule:                   snapCfg.Spec.Schedule,
			ConcurrencyPolicy:          batchv1.ForbidConcurrent,
			SuccessfulJobsHistoryLimit: &successHistory,
			FailedJobsHistoryLimit:     &failedHistory,
			JobTemplate: batchv1.JobTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: batchv1.JobSpec{
					BackoffLimit:          &backoffLimit,
					ActiveDeadlineSeconds: int64Ptr(int64(activeDeadline)),
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: labels},
						Spec: corev1.PodSpec{
							RestartPolicy:    corev1.RestartPolicyNever,
							ImagePullSecrets: c.Spec.ImagePullSecrets,
							Tolerations:      c.Spec.Tolerations,
							NodeSelector:     c.Spec.NodeSelector,
							Containers: []corev1.Container{
								{
									Name:            "backup-trigger",
									Image:           agentImage(c),
									ImagePullPolicy: pullPolicyOrDefault(c.Spec.ImagePullPolicy),
									Args: []string{
										"backup-trigger",
										"--url", backupURL,
										"--timeout", fmt.Sprintf("%ds", timeout),
									},
									Resources: resources,
								},
							},
						},
					},
				},
			},
		},
	}
}

func int64Ptr(v int64) *int64 { return &v }

// snapshotName gives the KividbSnapshot record created for a given backup
// Job the same name as the Job itself -- Job names are already unique per
// run (Kubernetes' CronJob controller suffixes them), so this needs no
// extra collision tracking.
func snapshotName(job *batchv1.Job) string { return job.Name }

// reconcileSnapshots creates or updates one KividbSnapshot per backup Job
// belonging to this cluster, deriving status from the Job's own
// conditions and (for the result the agent actually produced) the
// backup-trigger container's termination message -- see
// cmd/agent/backup_trigger.go's writeBackupResult. This is what makes
// `kubectl get kividbsnapshot` a real, queryable backup history instead
// of just the CronJob's own coarse lastSuccessfulTime.
func (r *KividbClusterReconciler) reconcileSnapshots(ctx context.Context, c *kividbv1alpha1.KividbCluster, snapshotConfigName string) error {
	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs, client.InNamespace(c.Namespace), client.MatchingLabels(backupLabels(c))); err != nil {
		return fmt.Errorf("listing backup jobs: %w", err)
	}

	for i := range jobs.Items {
		job := &jobs.Items[i]
		if err := r.reconcileOneSnapshot(ctx, c, snapshotConfigName, job); err != nil {
			return fmt.Errorf("job %s: %w", job.Name, err)
		}
	}
	return nil
}

func (r *KividbClusterReconciler) reconcileOneSnapshot(ctx context.Context, c *kividbv1alpha1.KividbCluster, snapshotConfigName string, job *batchv1.Job) error {
	snap := &kividbv1alpha1.KividbSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: snapshotName(job), Namespace: c.Namespace},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, snap, func() error {
		if snap.Labels == nil {
			snap.Labels = map[string]string{}
		}
		snap.Labels[kividbv1alpha1.ClusterLabel] = c.Name
		snap.Labels["kividb.io/snapshot-config"] = snapshotConfigName
		snap.Spec.ClusterRef = corev1.LocalObjectReference{Name: c.Name}
		snap.Spec.SnapshotConfigRef = corev1.LocalObjectReference{Name: snapshotConfigName}
		// Deliberately no OwnerReference to the KividbCluster: backup
		// history should survive cluster deletion (the S3 objects
		// certainly do), not cascade-delete with it.
		return nil
	})
	if err != nil {
		return err
	}

	// KividbSnapshot has a status subresource: CreateOrUpdate's own Update
	// call above cannot persist .Status changes (the API server silently
	// ignores them there), so status is populated and written separately.
	populateSnapshotStatus(ctx, r.Client, job, snap)
	return r.Status().Update(ctx, snap)
}

// populateSnapshotStatus derives status purely from Kubernetes-native
// state: the Job's own conditions/timestamps, and (once available) the
// backup-trigger container's termination message on its most recent pod.
func populateSnapshotStatus(ctx context.Context, c client.Client, job *batchv1.Job, snap *kividbv1alpha1.KividbSnapshot) {
	if job.Status.StartTime != nil {
		snap.Status.StartTime = job.Status.StartTime
	}

	var failed, complete *batchv1.JobCondition
	for i := range job.Status.Conditions {
		cond := &job.Status.Conditions[i]
		if cond.Status != corev1.ConditionTrue {
			continue
		}
		switch cond.Type {
		case batchv1.JobFailed:
			failed = cond
		case batchv1.JobComplete:
			complete = cond
		}
	}

	switch {
	case complete != nil:
		snap.Status.Phase = kividbv1alpha1.SnapshotSucceeded
		snap.Status.CompletionTime = &complete.LastTransitionTime
	case failed != nil:
		snap.Status.Phase = kividbv1alpha1.SnapshotFailed
		snap.Status.CompletionTime = &failed.LastTransitionTime
		snap.Status.Error = failed.Message
	default:
		snap.Status.Phase = kividbv1alpha1.SnapshotInProgress
	}

	result, ok := latestBackupResult(ctx, c, job)
	if !ok {
		return
	}
	if result.Error != "" {
		snap.Status.Error = result.Error
	}
	snap.Status.ObjectKey = result.ObjectKey
	snap.Status.SizeBytes = result.SizeBytes
	snap.Status.DurationMs = result.DurationMs
	snap.Status.SourcePod = result.SourcePod
	snap.Status.SourceRole = kividbv1alpha1.NodeRole(result.SourceRole)
}

// latestBackupResult finds the most recent pod belonging to job and reads
// its backup-trigger container's termination message, if the pod has
// actually terminated yet.
func latestBackupResult(ctx context.Context, c client.Client, job *batchv1.Job) (agentapi.BackupResult, bool) {
	var pods corev1.PodList
	if err := c.List(ctx, &pods, client.InNamespace(job.Namespace), client.MatchingLabels{"job-name": job.Name}); err != nil || len(pods.Items) == 0 {
		return agentapi.BackupResult{}, false
	}
	sort.Slice(pods.Items, func(i, j int) bool {
		return pods.Items[j].CreationTimestamp.Before(&pods.Items[i].CreationTimestamp)
	})

	for _, cs := range pods.Items[0].Status.ContainerStatuses {
		if cs.Name != "backup-trigger" || cs.State.Terminated == nil {
			continue
		}
		var result agentapi.BackupResult
		if err := json.Unmarshal([]byte(cs.State.Terminated.Message), &result); err != nil {
			continue
		}
		return result, true
	}
	return agentapi.BackupResult{}, false
}

// reconcileBackupCronJob creates/updates the backup CronJob from the
// already-resolved snapCfg (nil means spec.snapshotConfigRef is unset, in
// which case any existing CronJob is deleted).
func (r *KividbClusterReconciler) reconcileBackupCronJob(ctx context.Context, c *kividbv1alpha1.KividbCluster, snapCfg *kividbv1alpha1.KividbSnapshotConfig) error {
	name := backupCronJobName(c)
	if snapCfg == nil {
		cj := &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: c.Namespace}}
		err := r.Delete(ctx, cj)
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}

	cj := &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: c.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cj, func() error {
		desired := desiredBackupCronJob(c, snapCfg)
		cj.Labels = desired.Labels
		cj.Spec = desired.Spec
		return controllerutil.SetControllerReference(c, cj, r.scheme())
	})
	return err
}
