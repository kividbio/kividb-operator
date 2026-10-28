package controller

import (
	"context"
	"sort"
	"strings"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// Changing the default user's password, or setting or removing it, without
// a moment in which a client or a replica is refused.
//
// The pods read the password at startup (the agent, the exporter and, in a
// sharded cluster, masterauth), so a change rolls them, one at a time. On
// its own that leaves two kinds of pod side by side for the whole rollout,
// each accepting only its own password: clients on the new password are
// refused by the old master until it is replaced, and replicas started
// with the new one (or with none, after a removal) cannot authenticate to
// it -- their link stays down. So the operator widens first and narrows
// last:
//
//  1. When the password changes, the previous credential is kept as an
//     overlap. Until the rollout is over, the ACL file renders the default
//     user with both the new and the previous credential -- "nopass" if
//     either is none, since a user without a password accepts any -- so
//     every pod started during the rollout accepts both.
//  2. Before any pod is restarted, the pods still running with the
//     previous password are given the same credentials live: ACL SETUSER
//     default #<hash> adds the new password next to the old one, ACL
//     SETUSER default nopass opens the user for a removal. Each is stamped
//     with AuthWidenedAnnotation. Clients and replicas on either password
//     are accepted everywhere from here on.
//  3. Once every pod runs with the new password, the overlap is dropped:
//     the ACL file is rendered with the new credential alone, and
//     reconcileAclReload has every pod load it. The previous password
//     stops working only then.

const (
	// defaultCredentialKey, in the operator's auth Secret, holds the
	// default user's credential as last rendered without the overlap:
	// "nopass" or "#<hash>".
	defaultCredentialKey = "default-user-credential"
	// defaultOverlapKey, in the auth Secret, holds the credentials kept
	// from before the password changed, until the rollout is over.
	defaultOverlapKey = "default-user-overlap"

	// AuthWidenedAnnotation, on a pod still running with the previous
	// password, records the auth generation whose credentials have been
	// added to its default user live.
	AuthWidenedAnnotation = "kividb.io/auth-widened"
)

// unionCredentials merges two lists of ACL credentials ("nopass",
// "#<hash>"): "nopass" alone if either has it -- a user without a password
// accepts any, the other one included -- else every hash once, sorted.
func unionCredentials(a, b []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, t := range append(append([]string(nil), a...), b...) {
		if t == "nopass" {
			return []string{"nopass"}
		}
		if t != "" && !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}
	sort.Strings(out)
	return out
}

// aclDefaultCredentials returns the credentials ("nopass", "#<hash>") of
// the default user in a rendered ACL file.
func aclDefaultCredentials(aclContent string) []string {
	for _, line := range strings.Split(aclContent, "\n") {
		f := strings.Fields(line)
		if len(f) < 2 || f[0] != "user" || f[1] != "default" {
			continue
		}
		var out []string
		for _, t := range f[2:] {
			if t == "nopass" || strings.HasPrefix(t, "#") {
				out = append(out, t)
			}
		}
		return out
	}
	return nil
}

// secretString reads key from s: StringData, which is what this operator
// wrote, before Data, which is what the API server stores it as.
func secretString(s *corev1.Secret, key string) string {
	if v, ok := s.StringData[key]; ok {
		return v
	}
	return string(s.Data[key])
}

func sameCredentials(a, b []string) bool {
	return strings.Join(unionCredentials(a, nil), " ") == strings.Join(unionCredentials(b, nil), " ")
}

// nextAuthOverlap returns the credentials to go on accepting next to
// those of base, the freshly rendered ACL file, given what the auth
// Secret recorded last time.
func nextAuthOverlap(existing *corev1.Secret, base string) []string {
	overlap := strings.Fields(secretString(existing, defaultOverlapKey))
	previous := strings.Fields(secretString(existing, defaultCredentialKey))
	if len(previous) == 0 {
		// Written by an operator before 0.7.0: what the pods were given is
		// in the ACL file itself.
		previous = aclDefaultCredentials(secretString(existing, AclFileName))
	}
	if current := aclDefaultCredentials(base); len(previous) > 0 && !sameCredentials(previous, current) {
		overlap = unionCredentials(overlap, previous)
	}
	return overlap
}

// reconcileAuthOverlap carries out steps 2 and 3 above. It reports whether
// the rollout may restart pods: not before every Ready pod still running
// with the previous password accepts the new one.
func (r *KividbClusterReconciler) reconcileAuthOverlap(ctx context.Context, c *kividbv1alpha1.KividbCluster, pods []corev1.Pod, generations authGenerations) bool {
	if len(generations.overlap) == 0 {
		return true
	}
	log := logf.FromContext(ctx)
	widened := true
	done := int32(len(pods)) == desiredPodCount(c)
	for i := range pods {
		p := &pods[i]
		if p.DeletionTimestamp != nil {
			done = false
			continue
		}
		if p.Annotations[AuthGenerationAnnotation] == generations.auth {
			if !isPodReady(p) {
				done = false
			}
			continue // started with the current ACL file, which has both
		}
		done = false
		if !isPodReady(p) || p.Annotations[AuthWidenedAnnotation] == generations.auth {
			continue
		}
		args := append([]string{"ACL", "SETUSER", "default"}, generations.defaultCredentials...)
		if _, err := r.Agent.Exec(ctx, p.Status.PodIP, args...); err != nil {
			log.Error(err, "adding the new password to a pod still on the previous one", "pod", p.Name)
			r.event(c, corev1.EventTypeWarning, "PasswordOverlapFailed", "could not make %s accept the new password of the default user before restarting it: %v", p.Name, err)
			widened = false
			continue
		}
		patch := client.MergeFrom(p.DeepCopy())
		if p.Annotations == nil {
			p.Annotations = map[string]string{}
		}
		p.Annotations[AuthWidenedAnnotation] = generations.auth
		if err := r.Patch(ctx, p, patch); err != nil {
			log.Error(err, "recording the password overlap", "pod", p.Name)
		}
		log.Info("the pod accepts the new and the previous password until the rollout is over", "pod", p.Name)
	}
	if done {
		r.endAuthOverlap(ctx, c)
	}
	return widened
}

// endAuthOverlap drops the overlap: the next reconcile renders the ACL file
// with the new credential alone, and every pod loads it.
func (r *KividbClusterReconciler) endAuthOverlap(ctx context.Context, c *kividbv1alpha1.KividbCluster) {
	var s corev1.Secret
	if err := r.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: secretName(c)}, &s); err != nil {
		if !apierrors.IsNotFound(err) {
			logf.FromContext(ctx).Error(err, "reading the auth Secret to end the password overlap")
		}
		return
	}
	patch := client.MergeFrom(s.DeepCopy())
	if s.StringData == nil {
		s.StringData = map[string]string{}
	}
	s.StringData[defaultOverlapKey] = ""
	if err := r.Patch(ctx, &s, patch); err != nil {
		logf.FromContext(ctx).Error(err, "ending the password overlap")
		return
	}
	r.event(c, corev1.EventTypeNormal, "PasswordChanged", "every pod runs with the new password of the default user; the previous one is no longer accepted once the pods have reloaded the ACL file")
}

// desiredPodCount is how many pods the cluster has when it is whole.
func desiredPodCount(c *kividbv1alpha1.KividbCluster) int32 {
	if isSharded(c) {
		return shardCount(c) * (c.Spec.Replicas + 1)
	}
	return c.Spec.Replicas + 1
}
