package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	"github.com/kividbio/kividb-operator/internal/agentapi"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// configHash fingerprints the rendered kividb.conf, which holds no
// credentials (the ACL file and passwords live elsewhere).
func configHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// aclFileFingerprint fingerprints the rendered ACL file the same way the
// agent fingerprints the file mounted in its pod.
func aclFileFingerprint(aclContent string) string {
	return agentapi.Fingerprint(aclContent, agentapi.AclFileFingerprintSalt)
}

// passwordFingerprint fingerprints the default user's password, salted
// per cluster, so that a change can be detected without keeping a fast
// hash of it.
func passwordFingerprint(c *kividbv1alpha1.KividbCluster, password string) string {
	return agentapi.Fingerprint(password, "kividb-operator/default-password/"+c.Namespace+"/"+c.Name)
}

// authGenerations are the current values of the two change counters kept
// on the operator's auth Secret; see AuthGenerationAnnotation and
// AclGenerationAnnotation.
type authGenerations struct {
	auth string
	acl  string
}

// bumpAuthGenerations records the hashes of the ACL file and the default
// user's password on secret, advancing the matching counter whenever one
// differs from what was recorded last time, and returns the counters.
func bumpAuthGenerations(c *kividbv1alpha1.KividbCluster, secret *corev1.Secret, aclContent, defaultPassword string) authGenerations {
	if secret.Annotations == nil {
		secret.Annotations = map[string]string{}
	}
	bump := func(hashKey, generationKey, hash string) string {
		generation := secret.Annotations[generationKey]
		if generation == "" || secret.Annotations[hashKey] != hash {
			n, _ := strconv.Atoi(generation)
			generation = strconv.Itoa(n + 1)
			secret.Annotations[hashKey] = hash
			secret.Annotations[generationKey] = generation
		}
		return generation
	}
	return authGenerations{
		auth: bump(AuthHashAnnotation, AuthGenerationAnnotation, passwordFingerprint(c, defaultPassword)),
		acl:  bump(AclHashAnnotation, AclGenerationAnnotation, aclFileFingerprint(aclContent)),
	}
}

// reconcileAclReload makes running pods pick up a changed ACL file.
//
// kividb reads its ACL file at startup and on ACL LOAD, never on its own.
// Updating the Secret therefore changes the file inside each pod (once the
// kubelet syncs the volume, typically within a minute or two) but not what
// kividb enforces: a rotated or revoked password keeps working until
// something reloads it. So for every Ready pod that has not yet loaded the
// current ACL generation, ask its agent to ACL LOAD -- which the agent
// only does once the file in that pod is the one rendered here -- and
// stamp the pod with the generation when it has.
//
// Pods whose template predates a change of the default user's password
// are skipped: their agent still authenticates with the old password, so
// loading the new ACL under it would lock the agent out and fail the pod's
// readiness probe. Those pods are being rolled by the StatefulSet (see
// AuthGenerationAnnotation) and load the new file when they start.
//
// Failures are logged and retried on the next reconcile, never returned:
// an ACL that is slow to apply must not hold up role management.
func (r *KividbClusterReconciler) reconcileAclReload(ctx context.Context, c *kividbv1alpha1.KividbCluster, pods []corev1.Pod, aclContent string, generations authGenerations) {
	log := logf.FromContext(ctx)
	fileHash := aclFileFingerprint(aclContent)

	for i := range pods {
		p := &pods[i]
		if !isPodReady(p) || p.DeletionTimestamp != nil {
			continue
		}
		if p.Annotations[AclGenerationAnnotation] == generations.acl {
			continue
		}
		if p.Annotations[AuthGenerationAnnotation] != generations.auth {
			continue
		}

		reloaded, err := r.Agent.AclReload(ctx, p.Status.PodIP, fileHash)
		if err != nil {
			log.Error(err, "ACL reload failed", "pod", p.Name)
			r.event(c, corev1.EventTypeWarning, "AclReloadFailed", "reloading the ACL file on %s: %v", p.Name, err)
			continue
		}
		if !reloaded {
			log.V(1).Info("updated ACL file has not reached the pod yet", "pod", p.Name)
			continue
		}

		patch := client.MergeFrom(p.DeepCopy())
		if p.Annotations == nil {
			p.Annotations = map[string]string{}
		}
		p.Annotations[AclGenerationAnnotation] = generations.acl
		if err := r.Client.Patch(ctx, p, patch); err != nil {
			log.Error(err, "recording loaded ACL generation", "pod", p.Name)
			continue
		}
		log.Info("ACL file reloaded", "pod", p.Name, "generation", generations.acl)
	}
}
