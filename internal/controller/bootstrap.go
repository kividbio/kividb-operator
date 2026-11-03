package controller

import (
	"context"
	"fmt"
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
		}
		return false, nil
	}

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
			if !bootstrapWaiting(c) {
				r.event(c, corev1.EventTypeWarning, "BootstrapWaiting",
					"spec.bootstrapFromSnapshot (%s) waits: StatefulSet %s, its pods or restore Job %s are left from an earlier cluster with this name, and the restore starts once they are deleted", snapName, statefulSetName(c), bootstrapJobName(c))
			}
			c.Status.Bootstrap = &kividbv1alpha1.BootstrapStatus{
				Phase:        kividbv1alpha1.BootstrapPending,
				SnapshotName: snapName,
				Message:      bootstrapWaitMessage,
			}
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

	kept, err := r.ensureBootstrapPVC(ctx, c)
	if err != nil {
		return true, err
	}

	job, err := r.ensureBootstrapJob(ctx, c, &snap, &snapCfg)
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
	if job.Status.Failed > 0 {
		c.Status.Bootstrap.Phase = kividbv1alpha1.BootstrapFailed
		c.Status.Bootstrap.Error = "bootstrap Job failed; check Job logs"
		c.Status.Bootstrap.Message = fmt.Sprintf("restore Job %s failed", job.Name)
		return true, fmt.Errorf("bootstrap job failed")
	}

	return true, nil
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
// this name, or made beforehand.
func (r *KividbClusterReconciler) ensureBootstrapPVC(ctx context.Context, c *kividbv1alpha1.KividbCluster) (kept bool, err error) {
	name := bootstrapPVCName(c)
	var pvc corev1.PersistentVolumeClaim
	err = r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: name}, &pvc)
	if err == nil {
		return pvc.CreationTimestamp.Before(&c.CreationTimestamp), nil
	}
	if !apierrors.IsNotFound(err) {
		return false, err
	}

	qty, err := resource.ParseQuantity(c.Spec.Storage.Size)
	if err != nil {
		return false, fmt.Errorf("storage.size: %w", err)
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
	return false, r.Create(ctx, &pvc)
}

func (r *KividbClusterReconciler) ensureBootstrapJob(ctx context.Context, c *kividbv1alpha1.KividbCluster, snap *kividbv1alpha1.KividbSnapshot, snapCfg *kividbv1alpha1.KividbSnapshotConfig) (*batchv1.Job, error) {
	name := bootstrapJobName(c)
	var existing batchv1.Job
	err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: name}, &existing)
	if err == nil {
		if !metav1.IsControlledBy(&existing, c) {
			// Left from an earlier cluster with this name: its result is
			// not this cluster's restore.
			return nil, fmt.Errorf("restore Job %s is not this cluster's but an earlier cluster's with this name; waiting for it to be deleted", name)
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
