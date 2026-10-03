package controller

import (
	"context"
	"fmt"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
		return false, nil
	}

	snapName := c.Spec.BootstrapFromSnapshot.SnapshotRef.Name
	if c.Status.Bootstrap == nil {
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

	var snapCfg kividbv1alpha1.KividbSnapshotConfig
	if err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: snap.Spec.SnapshotConfigRef.Name}, &snapCfg); err != nil {
		c.Status.Bootstrap.Phase = kividbv1alpha1.BootstrapFailed
		c.Status.Bootstrap.Error = fmt.Sprintf("snapshotConfig %q: %v", snap.Spec.SnapshotConfigRef.Name, err)
		return true, err
	}

	if err := r.ensureBootstrapPVC(ctx, c); err != nil {
		return true, err
	}

	job, err := r.ensureBootstrapJob(ctx, c, &snap, &snapCfg)
	if err != nil {
		return true, err
	}

	c.Status.Bootstrap.Phase = kividbv1alpha1.BootstrapInProgress
	c.Status.Bootstrap.SnapshotName = snapName
	c.Status.Bootstrap.Message = "restore Job running"

	if job.Status.Succeeded > 0 {
		now := metav1.Now()
		c.Status.Bootstrap.Phase = kividbv1alpha1.BootstrapCompleted
		c.Status.Bootstrap.Completed = true
		c.Status.Bootstrap.CompletionTime = &now
		c.Status.Bootstrap.Message = "PVC seeded from snapshot"
		c.Status.Bootstrap.Error = ""
		log.Info("bootstrap completed", "cluster", c.Name, "snapshot", snapName)
		return false, nil
	}
	if job.Status.Failed > 0 {
		c.Status.Bootstrap.Phase = kividbv1alpha1.BootstrapFailed
		c.Status.Bootstrap.Error = "bootstrap Job failed; check Job logs"
		return true, fmt.Errorf("bootstrap job failed")
	}

	return true, nil
}

func (r *KividbClusterReconciler) ensureBootstrapPVC(ctx context.Context, c *kividbv1alpha1.KividbCluster) error {
	name := bootstrapPVCName(c)
	var pvc corev1.PersistentVolumeClaim
	err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: name}, &pvc)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	qty, err := resource.ParseQuantity(c.Spec.Storage.Size)
	if err != nil {
		return fmt.Errorf("storage.size: %w", err)
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
	return r.Create(ctx, &pvc)
}

func (r *KividbClusterReconciler) ensureBootstrapJob(ctx context.Context, c *kividbv1alpha1.KividbCluster, snap *kividbv1alpha1.KividbSnapshot, snapCfg *kividbv1alpha1.KividbSnapshotConfig) (*batchv1.Job, error) {
	name := bootstrapJobName(c)
	var existing batchv1.Job
	err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: name}, &existing)
	if err == nil {
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
