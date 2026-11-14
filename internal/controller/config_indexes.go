package controller

import (
	"context"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// ClusterConfigRefIndex indexes KividbCluster by spec.configRef.name.
	ClusterConfigRefIndex = ".spec.configRef.name"

	// ClusterAclConfigRefIndex indexes KividbCluster by spec.aclConfigRef.name.
	ClusterAclConfigRefIndex = ".spec.aclConfigRef.name"

	// ClusterSnapshotConfigRefIndex indexes KividbCluster by spec.snapshotConfigRef.name.
	ClusterSnapshotConfigRefIndex = ".spec.snapshotConfigRef.name"
)

// SetupConfigIndexes registers field indexers on KividbCluster for its referenced
// KividbConfig, KividbAclConfig, and KividbSnapshotConfig objects.
func SetupConfigIndexes(ctx context.Context, indexer client.FieldIndexer) error {
	if err := indexer.IndexField(ctx, &kividbv1alpha1.KividbCluster{}, ClusterConfigRefIndex, func(obj client.Object) []string {
		c, ok := obj.(*kividbv1alpha1.KividbCluster)
		if !ok || c.Spec.ConfigRef == nil || c.Spec.ConfigRef.Name == "" {
			return nil
		}
		return []string{c.Spec.ConfigRef.Name}
	}); err != nil {
		return err
	}

	if err := indexer.IndexField(ctx, &kividbv1alpha1.KividbCluster{}, ClusterAclConfigRefIndex, func(obj client.Object) []string {
		c, ok := obj.(*kividbv1alpha1.KividbCluster)
		if !ok || c.Spec.AclConfigRef == nil || c.Spec.AclConfigRef.Name == "" {
			return nil
		}
		return []string{c.Spec.AclConfigRef.Name}
	}); err != nil {
		return err
	}

	if err := indexer.IndexField(ctx, &kividbv1alpha1.KividbCluster{}, ClusterSnapshotConfigRefIndex, func(obj client.Object) []string {
		c, ok := obj.(*kividbv1alpha1.KividbCluster)
		if !ok || c.Spec.SnapshotConfigRef == nil || c.Spec.SnapshotConfigRef.Name == "" {
			return nil
		}
		return []string{c.Spec.SnapshotConfigRef.Name}
	}); err != nil {
		return err
	}

	return nil
}
