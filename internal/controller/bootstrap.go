package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

func bootstrapPVCName(c *kividbv1alpha1.KividbCluster) string {
	return fmt.Sprintf("data-%s-0", c.Name)
}

func bootstrapJobName(c *kividbv1alpha1.KividbCluster) string {
	return c.Name + "-bootstrap"
}

// BootstrapCondition says whether pod-0's volume was seeded from
// spec.bootstrapFromSnapshot and, until it is, what the bootstrap is doing
// or waiting for: reason WaitingForEarlierCluster, WaitingForVolumeDeletion,
// Pending, Restoring, RestoreRetrying, RestoreFailed; True with Restored or
// Skipped.
const BootstrapCondition = "Bootstrapped"

// errEarlierRestoreJob is returned by ensureBootstrapJob for a restore Job
// with this cluster's name that an earlier cluster with the name made.
var errEarlierRestoreJob = errors.New("not this cluster's but an earlier cluster's with this name; waiting for it to be deleted")

// reconcileBootstrap seeds pod-0's PVC from spec.bootstrapFromSnapshot
// before the StatefulSet is allowed to run. Returns shouldBlock=true while
// bootstrap is incomplete so the caller can skip role reconciliation / keep
// the STS at 0 replicas.
func (r *KividbClusterReconciler) reconcileBootstrap(ctx context.Context, c *kividbv1alpha1.KividbCluster) (shouldBlock bool, err error) {
	log := logf.FromContext(ctx)

	if c.Status.Bootstrap != nil && c.Status.Bootstrap.Completed {
		return false, nil
	}
	if c.Spec.BootstrapFromSnapshot == nil {
		if b := c.Status.Bootstrap; b != nil && b.Phase == kividbv1alpha1.BootstrapPending && b.Message == bootstrapWaitMessage {
			c.Status.Bootstrap = nil // removed while waiting: nothing was restored
			removeCondition(&c.Status.Conditions, BootstrapCondition)
		}
		return false, nil
	}

	// However the pass ends, the Bootstrapped condition says where the
	// bootstrap is. reason, when set, is what it waits for or does.
	var reason string
	defer func() { setBootstrapCondition(c, reason) }()

	snapName := c.Spec.BootstrapFromSnapshot.SnapshotRef.Name
	if c.Status.Bootstrap == nil || bootstrapWaiting(c) {
		// A StatefulSet not controlled by this cluster's UID, or being
		// deleted, is left from an earlier cluster with this name, and so is
		// a restore Job not controlled by it: wait until they are gone.
		var sts appsv1.StatefulSet
		err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: statefulSetName(c)}, &sts)
		if err != nil && !apierrors.IsNotFound(err) {
			return true, err
		}
		found := err == nil
		earlier := found && (!metav1.IsControlledBy(&sts, c) || sts.DeletionTimestamp != nil)
		if !found {
			// No StatefulSet of ours yet, so a pod with this cluster's
			// labels is the earlier cluster's, possibly still on pod-0's PVC.
			var pods corev1.PodList
			if err := r.List(ctx, &pods, client.InNamespace(c.Namespace), client.MatchingLabels(selectorLabels(c))); err != nil {
				return true, err
			}
			earlier = len(pods.Items) > 0
		}
		ourJob := false
		if !earlier {
			var job batchv1.Job
			err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: bootstrapJobName(c)}, &job)
			if err != nil && !apierrors.IsNotFound(err) {
				return true, err
			}
			earlier = err == nil && !metav1.IsControlledBy(&job, c)
			ourJob = err == nil && !earlier
		}
		if earlier {
			reason = "WaitingForEarlierCluster"
			r.waitForEarlierCluster(c, snapName)
			return true, nil
		}
		// The bootstrap holds its own StatefulSet at 0 replicas until the
		// restore is done (desiredSTSReplicas), so one of ours at 0 is the
		// bootstrap's: the restore goes on, whatever the status says. With
		// this cluster's own restore Job there, one of ours above 0 is the
		// restored one, scaled up after a completion this read is too old
		// to show. Otherwise one of ours above 0 has run without the
		// bootstrap: this cluster is already running, and restoring would
		// scale it to 0 and overwrite pod-0.
		if found && !ourJob && (sts.Spec.Replicas == nil || *sts.Spec.Replicas > 0) {
			r.event(c, corev1.EventTypeWarning, "BootstrapSkipped",
				"spec.bootstrapFromSnapshot (%s) ignored: the cluster is already running, and bootstrap only seeds a new cluster", snapName)
			c.Status.Bootstrap = &kividbv1alpha1.BootstrapStatus{
				Phase:        kividbv1alpha1.BootstrapCompleted,
				Completed:    true,
				SnapshotName: snapName,
				Message:      "skipped: the cluster was already running",
			}
			reason = "Skipped"
			return false, nil
		}
	}
	if c.Status.Bootstrap == nil || bootstrapWaiting(c) {
		c.Status.Bootstrap = &kividbv1alpha1.BootstrapStatus{
			Phase:        kividbv1alpha1.BootstrapPending,
			SnapshotName: snapName,
			Message:      "bootstrap requested",
		}
	}

	var snap kividbv1alpha1.KividbSnapshot
	if err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: snapName}, &snap); err != nil {
		c.Status.Bootstrap.Phase = kividbv1alpha1.BootstrapFailed
		c.Status.Bootstrap.Error = fmt.Sprintf("snapshot %q: %v", snapName, err)
		return true, fmt.Errorf("bootstrap snapshot: %w", err)
	}
	if snap.Status.Phase != kividbv1alpha1.SnapshotSucceeded || snap.Status.ObjectKey == "" {
		c.Status.Bootstrap.Phase = kividbv1alpha1.BootstrapFailed
		c.Status.Bootstrap.Error = fmt.Sprintf("snapshot %q is not Succeeded with an objectKey (phase=%s)", snapName, snap.Status.Phase)
		return true, fmt.Errorf("%s", c.Status.Bootstrap.Error)
	}
	if err := bootstrapSnapshotUsable(&snap); err != nil {
		// Refused before any Job runs: a restore Job would only fail on
		// it ("gzip: invalid header", a manifest is no archive), and
		// leave the cluster saying "restore Job running".
		if c.Status.Bootstrap.Phase != kividbv1alpha1.BootstrapFailed {
			r.event(c, corev1.EventTypeWarning, "BootstrapRefused", "%v", err)
		}
		c.Status.Bootstrap.Phase = kividbv1alpha1.BootstrapFailed
		c.Status.Bootstrap.Error = err.Error()
		c.Status.Bootstrap.Message = "bootstrap refused"
		return true, err
	}

	var snapCfg kividbv1alpha1.KividbSnapshotConfig
	if err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: snap.Spec.SnapshotConfigRef.Name}, &snapCfg); err != nil {
		c.Status.Bootstrap.Phase = kividbv1alpha1.BootstrapFailed
		c.Status.Bootstrap.Error = fmt.Sprintf("snapshotConfig %q: %v", snap.Spec.SnapshotConfigRef.Name, err)
		return true, err
	}

	kept, deleting, err := r.ensureBootstrapPVC(ctx, c)
	if err != nil {
		return true, err
	}
	if deleting {
		// A restore Job started now would have its pod refused the
		// volume, or restore onto one about to go.
		reason = "WaitingForVolumeDeletion"
		r.noteBootstrapWait(ctx, c, snapName, reason,
			fmt.Sprintf("waiting for PVC %s to finish deleting; the restore starts once it is gone", bootstrapPVCName(c)))
		return true, nil
	}

	job, err := r.ensureBootstrapJob(ctx, c, &snap, &snapCfg)
	if errors.Is(err, errEarlierRestoreJob) {
		reason = "WaitingForEarlierCluster"
		r.waitForEarlierCluster(c, snapName)
		return true, nil
	}
	if err != nil {
		return true, err
	}

	// A PVC older than this cluster was kept from an earlier one with its
	// name (the StatefulSet retains its PVCs), or made beforehand on
	// purpose. The restore replaces its data, as the spec asks: say so,
	// where it is seen.
	var over string
	if kept {
		over = fmt.Sprintf(": restoring over the existing volume %s, which is older than this cluster", bootstrapPVCName(c))
		if c.Status.Bootstrap.Phase != kividbv1alpha1.BootstrapInProgress {
			r.event(c, corev1.EventTypeWarning, "BootstrapOverExistingVolume",
				"spec.bootstrapFromSnapshot (%s)%s; its data is replaced by the snapshot's", snapName, over)
		}
	}
	prevPhase, prevMessage := c.Status.Bootstrap.Phase, c.Status.Bootstrap.Message
	c.Status.Bootstrap.Phase = kividbv1alpha1.BootstrapInProgress
	c.Status.Bootstrap.SnapshotName = snapName
	c.Status.Bootstrap.Message = "restore Job running" + over

	if job.Status.Succeeded > 0 {
		now := metav1.Now()
		c.Status.Bootstrap.Phase = kividbv1alpha1.BootstrapCompleted
		c.Status.Bootstrap.Completed = true
		c.Status.Bootstrap.CompletionTime = &now
		c.Status.Bootstrap.Message = "PVC seeded from snapshot" + over
		c.Status.Bootstrap.Error = ""
		log.Info("bootstrap completed", "cluster", c.Name, "snapshot", snapName)
		return false, nil
	}
	// A pod that failed is not a failed Job: the Job retries it until
	// backoffLimit is used up (or activeDeadlineSeconds has passed), and
	// only then is its JobFailed condition true (issue #54).
	if failed := jobFailedCondition(job); failed != nil {
		why := "bootstrap Job failed"
		if failed.Reason != "" {
			why += " (" + failed.Reason
			if failed.Message != "" {
				why += ": " + failed.Message
			}
			why += ")"
		}
		if pod := r.lastRestorePodFailure(ctx, c, job); pod != "" {
			why += "; last " + pod
		}
		c.Status.Bootstrap.Phase = kividbv1alpha1.BootstrapFailed
		c.Status.Bootstrap.Error = why + "; check Job logs"
		c.Status.Bootstrap.Message = fmt.Sprintf("restore Job %s failed", job.Name) + over
		if prevPhase != kividbv1alpha1.BootstrapFailed {
			r.event(c, corev1.EventTypeWarning, "BootstrapFailed", "restore Job %s failed: %s", job.Name, c.Status.Bootstrap.Error)
		}
		return true, fmt.Errorf("bootstrap job failed")
	}
	if job.Status.Failed > 0 {
		attempt := fmt.Sprintf("restore Job attempt %d failed, retrying", job.Status.Failed)
		msg := attempt
		if pod := r.lastRestorePodFailure(ctx, c, job); pod != "" {
			msg += " (" + pod + ")"
		}
		reason = "RestoreRetrying"
		c.Status.Bootstrap.Message = msg + over
		if !strings.HasPrefix(prevMessage, attempt) {
			r.event(c, corev1.EventTypeWarning, "BootstrapRetrying", "%s", msg)
		}
	}

	return true, nil
}

// waitForEarlierCluster records that the bootstrap waits for the
// StatefulSet, pods or restore Job of an earlier cluster with this name to
// be deleted, with a Warning Event when it starts to.
func (r *KividbClusterReconciler) waitForEarlierCluster(c *kividbv1alpha1.KividbCluster, snapName string) {
	if !bootstrapWaiting(c) {
		r.event(c, corev1.EventTypeWarning, "BootstrapWaiting",
			"spec.bootstrapFromSnapshot (%s) waits: StatefulSet %s, its pods or restore Job %s are left from an earlier cluster with this name, and the restore starts once they are deleted", snapName, statefulSetName(c), bootstrapJobName(c))
	}
	c.Status.Bootstrap = &kividbv1alpha1.BootstrapStatus{
		Phase:        kividbv1alpha1.BootstrapPending,
		SnapshotName: snapName,
		Message:      bootstrapWaitMessage,
	}
}

// noteBootstrapWait records in status.bootstrap that the bootstrap waits,
// and why (issue #55). The message is saved with the status, so the Normal
// Event and the log line come once, when the wait starts, not every pass.
func (r *KividbClusterReconciler) noteBootstrapWait(ctx context.Context, c *kividbv1alpha1.KividbCluster, snapName, reason, message string) {
	b := c.Status.Bootstrap
	if b.Message != message {
		r.event(c, corev1.EventTypeNormal, "BootstrapWaitingForVolume", "spec.bootstrapFromSnapshot (%s): %s", snapName, message)
		logf.FromContext(ctx).Info("bootstrap waiting", "cluster", c.Name, "reason", reason, "message", message)
	}
	b.Phase = kividbv1alpha1.BootstrapPending
	b.SnapshotName = snapName
	b.Message = message
	b.Error = ""
}

// setBootstrapCondition sets the Bootstrapped condition from
// status.bootstrap. reason, when set, names what a bootstrap that is not
// done waits for or does.
func setBootstrapCondition(c *kividbv1alpha1.KividbCluster, reason string) {
	b := c.Status.Bootstrap
	if b == nil {
		removeCondition(&c.Status.Conditions, BootstrapCondition)
		return
	}
	cond := metav1.Condition{
		Type:               BootstrapCondition,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            b.Message,
		ObservedGeneration: c.Generation,
	}
	switch {
	case b.Completed:
		cond.Status = metav1.ConditionTrue
		if reason != "Skipped" {
			cond.Reason = "Restored"
		}
	case b.Phase == kividbv1alpha1.BootstrapFailed:
		cond.Reason = "RestoreFailed"
		if b.Error != "" {
			cond.Message = b.Message + ": " + b.Error
		}
	case reason != "":
	case b.Phase == kividbv1alpha1.BootstrapInProgress:
		cond.Reason = "Restoring"
	default:
		cond.Reason = "Pending"
	}
	setCondition(&c.Status.Conditions, cond)
}

// jobFailedCondition returns the Job's JobFailed condition if it is true:
// the Job has given up.
func jobFailedCondition(job *batchv1.Job) *batchv1.JobCondition {
	for i := range job.Status.Conditions {
		if cond := &job.Status.Conditions[i]; cond.Type == batchv1.JobFailed && cond.Status == corev1.ConditionTrue {
			return cond
		}
	}
	return nil
}

// lastRestorePodFailure says which of the restore Job's pods failed last
// and why ("pod x: Error, exit code 1: <its last log line>"), or "" if no
// failed pod of it is seen.
func (r *KividbClusterReconciler) lastRestorePodFailure(ctx context.Context, c *kividbv1alpha1.KividbCluster, job *batchv1.Job) string {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(c.Namespace), client.MatchingLabels(bootstrapLabels(c))); err != nil {
		return ""
	}
	var failed []*corev1.Pod
	for i := range pods.Items {
		p := &pods.Items[i]
		ref := metav1.GetControllerOf(p)
		if p.Status.Phase != corev1.PodFailed || ref == nil || ref.Kind != "Job" || ref.Name != job.Name || (job.UID != "" && ref.UID != job.UID) {
			continue
		}
		failed = append(failed, p)
	}
	if len(failed) == 0 {
		return ""
	}
	sort.Slice(failed, func(i, j int) bool {
		if !failed[i].CreationTimestamp.Equal(&failed[j].CreationTimestamp) {
			return failed[i].CreationTimestamp.Before(&failed[j].CreationTimestamp)
		}
		return failed[i].Name < failed[j].Name
	})
	p := failed[len(failed)-1]
	if p.Status.Reason != "" {
		return fmt.Sprintf("pod %s: %s", p.Name, joinNonEmpty(p.Status.Reason, lastLine(p.Status.Message)))
	}
	for _, cs := range p.Status.ContainerStatuses {
		if t := cs.State.Terminated; t != nil && t.ExitCode != 0 {
			return fmt.Sprintf("pod %s: %s", p.Name, joinNonEmpty(fmt.Sprintf("%s, exit code %d", t.Reason, t.ExitCode), lastLine(t.Message)))
		}
	}
	return "pod " + p.Name
}

// lastLine is the last non-empty line of s, at most 200 bytes of it.
func lastLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	line := strings.TrimSpace(lines[len(lines)-1])
	if len(line) > 200 {
		line = line[:200] + "..."
	}
	return line
}

func joinNonEmpty(a, b string) string {
	if b == "" {
		return a
	}
	return a + ": " + b
}

// bootstrapSnapshotUsable refuses a snapshot a replicated cluster cannot be
// seeded from: a sharded cluster's backup run, whose object is a manifest
// naming one archive per shard, each holding a part of the hash slots.
func bootstrapSnapshotUsable(snap *kividbv1alpha1.KividbSnapshot) error {
	if len(snap.Status.Shards) > 0 || strings.HasSuffix(snap.Status.ObjectKey, "/manifest.json") {
		return fmt.Errorf("snapshot %q is a sharded cluster's backup (%d shards, manifest %s): a replicated cluster cannot be seeded from it, and restoring a sharded cluster is not supported yet; restore one shard's archive by hand (docs/BACKUP_RESTORE.md)",
			snap.Name, len(snap.Status.Shards), snap.Status.ObjectKey)
	}
	return nil
}

// ensureBootstrapPVC creates pod-0's PVC if it does not exist. kept is true
// for one created before this cluster: kept from an earlier cluster with
// this name, or made beforehand. deleting is true for one being deleted,
// which the caller waits for.
func (r *KividbClusterReconciler) ensureBootstrapPVC(ctx context.Context, c *kividbv1alpha1.KividbCluster) (kept, deleting bool, err error) {
	name := bootstrapPVCName(c)
	var pvc corev1.PersistentVolumeClaim
	err = r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: name}, &pvc)
	if err == nil {
		if pvc.DeletionTimestamp != nil {
			return false, true, nil
		}
		return pvc.CreationTimestamp.Before(&c.CreationTimestamp), false, nil
	}
	if !apierrors.IsNotFound(err) {
		return false, false, err
	}

	qty, err := resource.ParseQuantity(c.Spec.Storage.Size)
	if err != nil {
		return false, false, fmt.Errorf("storage.size: %w", err)
	}
	accessModes := c.Spec.Storage.AccessModes
	if len(accessModes) == 0 {
		accessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	}

	pvc = corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: c.Namespace,
			Labels:    commonLabels(c),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: accessModes,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: qty},
			},
		},
	}
	if c.Spec.Storage.StorageClassName != nil {
		pvc.Spec.StorageClassName = c.Spec.Storage.StorageClassName
	}
	// Deliberately no OwnerReference: this is the same PVC the StatefulSet's
	// volumeClaimTemplate would have created for pod-0 (it adopts it by
	// name), and those are retained when the cluster is deleted. Owning this
	// one would make pod-0's data the only volume garbage-collected with
	// the KividbCluster.
	return false, false, r.Create(ctx, &pvc)
}

func (r *KividbClusterReconciler) ensureBootstrapJob(ctx context.Context, c *kividbv1alpha1.KividbCluster, snap *kividbv1alpha1.KividbSnapshot, snapCfg *kividbv1alpha1.KividbSnapshotConfig) (*batchv1.Job, error) {
	name := bootstrapJobName(c)
	var existing batchv1.Job
	err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: name}, &existing)
	if err == nil {
		if !metav1.IsControlledBy(&existing, c) {
			// Left from an earlier cluster with this name: its result is
			// not this cluster's restore.
			return nil, fmt.Errorf("restore Job %s: %w", name, errEarlierRestoreJob)
		}
		return &existing, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}

	accessKeyKey := snapCfg.Spec.S3.CredentialsSecretRef.AccessKeyIDKey
	if accessKeyKey == "" {
		accessKeyKey = "accessKeyId"
	}
	secretKeyKey := snapCfg.Spec.S3.CredentialsSecretRef.SecretAccessKeyKey
	if secretKeyKey == "" {
		secretKeyKey = "secretAccessKey"
	}

	backoff := int32(1)
	ttl := int32(86400)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: c.Namespace,
			Labels:    bootstrapLabels(c),
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoff,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: bootstrapLabels(c)},
				Spec: corev1.PodSpec{
					RestartPolicy:    corev1.RestartPolicyNever,
					ImagePullSecrets: c.Spec.ImagePullSecrets,
					Tolerations:      c.Spec.Tolerations,
					NodeSelector:     c.Spec.NodeSelector,
					Containers: []corev1.Container{{
						Name:            "restore",
						Image:           agentImage(c),
						ImagePullPolicy: pullPolicyOrDefault(c.Spec.ImagePullPolicy),
						// The end of the log of a failed restore is its
						// termination message, which status.bootstrap quotes.
						TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
						Args: []string{
							"restore-from-s3",
							"--endpoint", snapCfg.Spec.S3.Endpoint,
							"--bucket", snapCfg.Spec.S3.Bucket,
							"--region", snapCfg.Spec.S3.Region,
							"--object-key", snap.Status.ObjectKey,
							"--data-dir", DataDir,
							"--force-path-style", fmt.Sprintf("%t", snapCfg.Spec.S3.ForcePathStyle),
							"--insecure-skip-tls-verify", fmt.Sprintf("%t", snapCfg.Spec.S3.InsecureSkipTLSVerify),
						},
						Env: []corev1.EnvVar{
							{
								Name: "S3_ACCESS_KEY_ID",
								ValueFrom: &corev1.EnvVarSource{
									SecretKeyRef: &corev1.SecretKeySelector{
										LocalObjectReference: corev1.LocalObjectReference{Name: snapCfg.Spec.S3.CredentialsSecretRef.Name},
										Key:                  accessKeyKey,
									},
								},
							},
							{
								Name: "S3_SECRET_ACCESS_KEY",
								ValueFrom: &corev1.EnvVarSource{
									SecretKeyRef: &corev1.SecretKeySelector{
										LocalObjectReference: corev1.LocalObjectReference{Name: snapCfg.Spec.S3.CredentialsSecretRef.Name},
										Key:                  secretKeyKey,
									},
								},
							},
						},
						VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: DataDir}},
					}},
					Volumes: []corev1.Volume{{
						Name: "data",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: bootstrapPVCName(c)},
						},
					}},
					// The restore container runs as the agent image's own
					// UID, not kividb's, so what it writes is only readable
					// by kividb through group permissions. fsGroup alone
					// does not guarantee the group: not every volume type
					// applies it (hostPath-backed provisioners don't), in
					// which case new files get the process's primary GID.
					// Pinning that GID to the same value every kividb pod
					// gets as fsGroup makes the restored files (written
					// group-read/writable, see cmd/agent/restore.go) usable
					// by kividb either way. runAsUser only restates the
					// agent image's own user: the kubelet refuses a
					// runAsGroup without one.
					SecurityContext: &corev1.PodSecurityContext{
						FSGroup:    int64Ptr(DataVolumeFSGroup),
						RunAsUser:  int64Ptr(AgentImageUID),
						RunAsGroup: int64Ptr(DataVolumeFSGroup),
					},
				},
			},
		},
	}
	if err := controllerutil.SetControllerReference(c, job, r.Scheme); err != nil {
		return nil, err
	}
	if err := r.Create(ctx, job); err != nil {
		return nil, err
	}
	return job, nil
}

// bootstrapWaitMessage is persisted state: bootstrapWaiting keys on it, so
// changing the text strands clusters already waiting.
const bootstrapWaitMessage = "waiting for the StatefulSet, pods and restore Job of an earlier cluster with this name to be deleted"

// bootstrapWaiting is true while the bootstrap waits for the StatefulSet,
// pods or restore Job of an earlier cluster with this name to be deleted.
func bootstrapWaiting(c *kividbv1alpha1.KividbCluster) bool {
	b := c.Status.Bootstrap
	return c.Spec.BootstrapFromSnapshot != nil && b != nil && b.Phase == kividbv1alpha1.BootstrapPending && b.Message == bootstrapWaitMessage
}

// bootstrapSeededPod names the pod whose volume reconcileBootstrap seeded
// from a snapshot, or "" if the cluster is not bootstrapped from one.
func bootstrapSeededPod(c *kividbv1alpha1.KividbCluster) string {
	if c.Spec.BootstrapFromSnapshot == nil {
		return ""
	}
	return statefulSetName(c) + "-0"
}

// bootstrapBlocksSTS is true when the StatefulSet must not run (or must
// stay scaled to 0) until PVC seeding finishes.
func bootstrapBlocksSTS(c *kividbv1alpha1.KividbCluster) bool {
	if c.Spec.BootstrapFromSnapshot == nil {
		return false
	}
	if c.Status.Bootstrap != nil && c.Status.Bootstrap.Completed {
		return false
	}
	return true
}

// desiredSTSReplicas returns 0 while bootstrap is blocking, otherwise the
// normal replicas+1 count.
func desiredSTSReplicas(c *kividbv1alpha1.KividbCluster) int32 {
	if bootstrapBlocksSTS(c) {
		return 0
	}
	return c.Spec.Replicas + 1
}
