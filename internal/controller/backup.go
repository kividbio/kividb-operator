package controller

import (
	"context"
	"fmt"
	"sort"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// desiredBackupCronJob builds the CronJob that triggers scheduled backups.
// The Job's single container is the *same agent image* running in
// "backup-trigger" client mode: it makes one HTTP POST to the cluster's
// master Service (which always resolves to whichever pod currently holds
// RoleLabel=master, failover-safe by construction) at /backup, waits for
// the response, and exits 0/non-zero accordingly. All S3 upload logic and
// credentials stay inside the agent sidecar that already owns the data
// directory -- the CronJob pod never touches the PVC or S3 credentials
// directly.
func desiredBackupCronJob(c *kividbv1alpha1.KividbCluster) *batchv1.CronJob {
	timeout := int32(900)
	if c.Spec.Backup.TimeoutSeconds != nil {
		timeout = *c.Spec.Backup.TimeoutSeconds
	}

	backupURL := fmt.Sprintf("http://%s.%s.svc:%d/backup", masterServiceName(c), c.Namespace, AgentPort)

	resources := c.Spec.Backup.JobResources
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

	return &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      backupCronJobName(c),
			Namespace: c.Namespace,
			Labels:    backupLabels(c),
		},
		Spec: batchv1.CronJobSpec{
			Schedule:                   c.Spec.Backup.Schedule,
			ConcurrencyPolicy:          batchv1.ForbidConcurrent,
			SuccessfulJobsHistoryLimit: &successHistory,
			FailedJobsHistoryLimit:     &failedHistory,
			JobTemplate: batchv1.JobTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: backupLabels(c)},
				Spec: batchv1.JobSpec{
					BackoffLimit:          &backoffLimit,
					ActiveDeadlineSeconds: int64Ptr(int64(activeDeadline)),
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: backupLabels(c)},
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

// reconcileBackupStatus mirrors the backup CronJob's own native status
// (lastScheduleTime/lastSuccessfulTime, which Kubernetes already tracks
// for us) plus the most recent backup Job's pass/fail outcome into
// status.backup, so `kubectl get kividbcluster` reflects backup health
// without having to separately `kubectl get cronjob`/`kubectl get jobs`.
//
// This does not attempt to recover the exact S3 object key a successful
// run uploaded (that would mean scraping the Job pod's stdout, which
// needs a plain client-go clientset with log-subresource access this
// reconciler doesn't otherwise need) -- LastObjectKey is intentionally
// left for a future iteration; see docs/BACKUP_RESTORE.md for how to
// find it today (`kubectl logs job/<job-name>`).
func (r *KividbClusterReconciler) reconcileBackupStatus(ctx context.Context, c *kividbv1alpha1.KividbCluster) error {
	if !c.Spec.Backup.Enabled {
		return nil
	}

	var cj batchv1.CronJob
	if err := r.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: backupCronJobName(c)}, &cj); err != nil {
		if apierrors.IsNotFound(err) {
			return nil // not created yet this pass; next reconcile will pick it up
		}
		return fmt.Errorf("fetching backup cronjob: %w", err)
	}
	c.Status.Backup.LastRunTime = cj.Status.LastScheduleTime
	c.Status.Backup.LastSuccessTime = cj.Status.LastSuccessfulTime

	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs, client.InNamespace(c.Namespace), client.MatchingLabels(backupLabels(c))); err != nil {
		return fmt.Errorf("listing backup jobs: %w", err)
	}
	if len(jobs.Items) == 0 {
		return nil
	}
	sort.Slice(jobs.Items, func(i, j int) bool {
		return jobs.Items[j].CreationTimestamp.Before(&jobs.Items[i].CreationTimestamp)
	})
	latest := jobs.Items[0]

	for _, cond := range latest.Status.Conditions {
		if cond.Type == batchv1.JobFailed && cond.Status == corev1.ConditionTrue {
			c.Status.Backup.LastError = fmt.Sprintf("job %s: %s: %s", latest.Name, cond.Reason, cond.Message)
			return nil
		}
		if cond.Type == batchv1.JobComplete && cond.Status == corev1.ConditionTrue {
			c.Status.Backup.LastError = ""
			return nil
		}
	}
	return nil
}
