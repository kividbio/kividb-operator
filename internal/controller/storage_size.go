package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// ConditionStorageResizePending is True while spec.storage.size asks
	// for more than the cluster's volumes were made with. A StatefulSet's
	// volume claim template cannot change, so the volumes are grown by hand
	// (docs/TROUBLESHOOTING.md); the condition goes once every StatefulSet
	// asks for the new size.
	ConditionStorageResizePending = "StorageResizePending"

	// ReasonStorageSizeIncreased is the reason of
	// ConditionStorageResizePending and of its Event.
	ReasonStorageSizeIncreased = "StorageSizeIncreased"
)

// checkStorageSize compares spec.storage.size with what the volume claim
// template of each of the cluster's StatefulSets asks for, which is fixed
// when the StatefulSet is made: a later change of the size was silently
// ignored.
//
// A smaller size is refused (shrunk): a volume cannot shrink, so the
// returned error holds the pass back as an invalid spec until the size is
// set back. A larger one sets ConditionStorageResizePending, with the
// manual steps, and a Warning Event each time its message changes; an equal
// one takes the condition off. err is a failure to list the StatefulSets.
func (r *KividbClusterReconciler) checkStorageSize(ctx context.Context, c *kividbv1alpha1.KividbCluster) (shrunk, err error) {
	want, err := resource.ParseQuantity(c.Spec.Storage.Size)
	if err != nil {
		return nil, nil // refused by validateSpec
	}
	var list appsv1.StatefulSetList
	if err := r.List(ctx, &list, client.InNamespace(c.Namespace),
		client.MatchingLabels{kividbv1alpha1.ClusterLabel: c.Name}); err != nil {
		return nil, fmt.Errorf("listing StatefulSets: %w", err)
	}
	var smaller, larger []string
	var made resource.Quantity
	for i := range list.Items {
		sts := &list.Items[i]
		if !metav1.IsControlledBy(sts, c) {
			continue
		}
		have, ok := dataClaimSize(sts)
		if !ok {
			continue
		}
		switch want.Cmp(have) {
		case -1:
			smaller = append(smaller, sts.Name)
			made = have
		case 1:
			larger = append(larger, sts.Name)
			made = have
		}
	}
	sort.Strings(smaller)
	sort.Strings(larger)

	if len(smaller) > 0 {
		removeCondition(&c.Status.Conditions, ConditionStorageResizePending)
		return fmt.Errorf("spec.storage.size %s is smaller than the %s the volumes of StatefulSet %s were made with: a volume cannot shrink; set spec.storage.size back to %s or more",
			c.Spec.Storage.Size, made.String(), strings.Join(smaller, ", "), made.String()), nil
	}
	if len(larger) == 0 {
		removeCondition(&c.Status.Conditions, ConditionStorageResizePending)
		return nil, nil
	}
	msg := fmt.Sprintf("spec.storage.size is %s, but the volumes of StatefulSet %s were made with %s, and a StatefulSet's volume claim template cannot change. "+
		"To grow them, with a StorageClass that has allowVolumeExpansion: true: patch each PVC of the cluster (kubectl get pvc -l %s=%s) to %s, "+
		"wait until each reports the new capacity, then delete the StatefulSet with --cascade=orphan so the operator recreates it with the new size (its pods keep running). "+
		"See docs/TROUBLESHOOTING.md",
		c.Spec.Storage.Size, strings.Join(larger, ", "), made.String(), kividbv1alpha1.ClusterLabel, c.Name, c.Spec.Storage.Size)
	prev := findCondition(c.Status.Conditions, ConditionStorageResizePending)
	if prev == nil || prev.Status != metav1.ConditionTrue || prev.Message != msg {
		r.event(c, corev1.EventTypeWarning, ReasonStorageSizeIncreased, "%s", msg)
	}
	setCondition(&c.Status.Conditions, metav1.Condition{
		Type:               ConditionStorageResizePending,
		Status:             metav1.ConditionTrue,
		Reason:             ReasonStorageSizeIncreased,
		Message:            msg,
		ObservedGeneration: c.Generation,
	})
	return nil, nil
}

// dataClaimSize is the storage the StatefulSet's "data" volume claim
// template asks for.
func dataClaimSize(sts *appsv1.StatefulSet) (resource.Quantity, bool) {
	for _, pvc := range sts.Spec.VolumeClaimTemplates {
		if pvc.Name != "data" {
			continue
		}
		q, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		return q, ok
	}
	return resource.Quantity{}, false
}
