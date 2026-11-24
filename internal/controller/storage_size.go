package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
// set back, to at least the largest size among them. A larger one sets
// ConditionStorageResizePending, with the manual steps. The condition is
// saved as soon as it changes, and its Warning Event recorded once it is:
// a pass that ends before the status write (during a bootstrap, say)
// would otherwise find no condition the next time and record the Event
// again. An equal size takes the condition off. err is a failure to list
// the StatefulSets or to save the condition.
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
	var smaller, larger []sized
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
			smaller = append(smaller, sized{sts.Name, have})
		case 1:
			larger = append(larger, sized{sts.Name, have})
		}
	}
	byName := func(s []sized) {
		sort.Slice(s, func(i, j int) bool { return s[i].name < s[j].name })
	}
	byName(smaller)
	byName(larger)

	if len(smaller) > 0 {
		removeCondition(&c.Status.Conditions, ConditionStorageResizePending)
		// The largest among them: with shards at 10Gi and 30Gi and the
		// spec at 20Gi, 30Gi, not whichever was looked at last.
		least := smaller[0].size
		for _, s := range smaller[1:] {
			if s.size.Cmp(least) > 0 {
				least = s.size
			}
		}
		return fmt.Errorf("spec.storage.size %s is smaller than the volumes of StatefulSet %s: a volume cannot shrink; set spec.storage.size back to %s or more",
			c.Spec.Storage.Size, withSizes(smaller), least.String()), nil
	}
	if len(larger) == 0 {
		removeCondition(&c.Status.Conditions, ConditionStorageResizePending)
		return nil, nil
	}
	msg := fmt.Sprintf("spec.storage.size is %s, but %s, and a StatefulSet's volume claim template cannot change. "+
		"To grow them, with a StorageClass that has allowVolumeExpansion: true: patch each PVC of the cluster (kubectl get pvc -l %s=%s) to %s, "+
		"wait until each reports the new capacity, then delete the StatefulSet with --cascade=orphan so the operator recreates it with the new size (its pods keep running). "+
		"See docs/TROUBLESHOOTING.md",
		c.Spec.Storage.Size, madeWith(larger), kividbv1alpha1.ClusterLabel, c.Name, c.Spec.Storage.Size)
	prev := findCondition(c.Status.Conditions, ConditionStorageResizePending)
	if prev != nil && prev.Status == metav1.ConditionTrue && prev.Message == msg {
		return nil, nil
	}
	setCondition(&c.Status.Conditions, metav1.Condition{
		Type:               ConditionStorageResizePending,
		Status:             metav1.ConditionTrue,
		Reason:             ReasonStorageSizeIncreased,
		Message:            msg,
		ObservedGeneration: c.Generation,
	})
	if err := r.Status().Update(ctx, c); err != nil {
		if apierrors.IsConflict(err) {
			// Changed meanwhile: the next pass finds the condition
			// unsaved, and saves it and records the Event then.
			return nil, nil
		}
		return nil, fmt.Errorf("saving %s: %w", ConditionStorageResizePending, err)
	}
	r.event(c, corev1.EventTypeWarning, ReasonStorageSizeIncreased, "%s", msg)
	return nil, nil
}

// sized is a StatefulSet and the size its "data" volume claim template
// asks for.
type sized struct {
	name string
	size resource.Quantity
}

// withSizes names each StatefulSet with its size: "c1-0 (10Gi), c1-1 (30Gi)".
func withSizes(sets []sized) string {
	names := make([]string, len(sets))
	for i, s := range sets {
		names[i] = fmt.Sprintf("%s (%s)", s.name, s.size.String())
	}
	return strings.Join(names, ", ")
}

// madeWith says what the volumes of the StatefulSets were made with: one
// size for them all, or each one's.
func madeWith(sets []sized) string {
	for _, s := range sets[1:] {
		if s.size.Cmp(sets[0].size) != 0 {
			return fmt.Sprintf("the volumes of StatefulSet %s were made with less", withSizes(sets))
		}
	}
	names := make([]string, len(sets))
	for i, s := range sets {
		names[i] = s.name
	}
	return fmt.Sprintf("the volumes of StatefulSet %s were made with %s", strings.Join(names, ", "), sets[0].size.String())
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
