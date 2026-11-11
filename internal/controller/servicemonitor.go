package controller

import (
	"context"
	"fmt"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// serviceMonitorGVK is the Prometheus Operator's ServiceMonitor. The
// operator does not depend on its Go types: it is read and written as an
// unstructured object, and only when the API server serves the kind.
var serviceMonitorGVK = schema.GroupVersionKind{Group: "monitoring.coreos.com", Version: "v1", Kind: "ServiceMonitor"}

// desiredServiceMonitorSpec scrapes /metrics on the cluster's headless
// Services: the replicated cluster's one, or a sharded cluster's one per
// shard. Each has every pod of the cluster (or shard) once. The agent's
// port is always there, the redis_exporter's with spec.monitoring.enabled.
func desiredServiceMonitorSpec(c *kividbv1alpha1.KividbCluster) map[string]interface{} {
	matchLabels := map[string]interface{}{ServiceKindLabel: "headless"}
	for k, v := range selectorLabels(c) {
		matchLabels[k] = v
	}
	endpoints := []interface{}{
		map[string]interface{}{"port": AgentPortName, "path": "/metrics"},
	}
	if c.Spec.Monitoring.Enabled {
		endpoints = append(endpoints, map[string]interface{}{"port": ExporterPortName, "path": "/metrics"})
	}
	return map[string]interface{}{
		"selector":  map[string]interface{}{"matchLabels": matchLabels},
		"endpoints": endpoints,
	}
}

// reconcileServiceMonitor makes the cluster's ServiceMonitor, named after
// it, when spec.monitoring.serviceMonitor is set, and deletes it when not.
// A ServiceMonitor of that name the cluster does not own is left alone:
// it is refused when the cluster wants one, and kept when it does not.
func (r *KividbClusterReconciler) reconcileServiceMonitor(ctx context.Context, c *kividbv1alpha1.KividbCluster) error {
	want := c.Spec.Monitoring.ServiceMonitor
	if _, err := r.RESTMapper().RESTMapping(serviceMonitorGVK.GroupKind(), serviceMonitorGVK.Version); err != nil {
		if !meta.IsNoMatchError(err) {
			return fmt.Errorf("looking up the ServiceMonitor kind: %w", err)
		}
		if want {
			return fmt.Errorf("spec.monitoring.serviceMonitor is set, but the API server does not serve servicemonitors.monitoring.coreos.com: install the Prometheus Operator's CRDs")
		}
		// No CRD, so no ServiceMonitor of ours to delete.
		return nil
	}

	sm := &unstructured.Unstructured{}
	sm.SetGroupVersionKind(serviceMonitorGVK)
	err := r.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: c.Name}, sm)
	switch {
	case apierrors.IsNotFound(err):
		if !want {
			return nil
		}
	case err != nil:
		return fmt.Errorf("reading ServiceMonitor %s: %w", c.Name, err)
	case !metav1.IsControlledBy(sm, c):
		if !want {
			return nil
		}
		return fmt.Errorf("ServiceMonitor %s already exists and is not owned by this cluster", c.Name)
	case !want:
		if err := r.Delete(ctx, sm); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("deleting ServiceMonitor %s: %w", c.Name, err)
		}
		return nil
	}

	sm = &unstructured.Unstructured{}
	sm.SetGroupVersionKind(serviceMonitorGVK)
	sm.SetNamespace(c.Namespace)
	sm.SetName(c.Name)
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, sm, func() error {
		sm.SetLabels(commonLabels(c))
		sm.Object["spec"] = desiredServiceMonitorSpec(c)
		return controllerutil.SetControllerReference(c, sm, r.scheme())
	})
	if err != nil {
		return fmt.Errorf("ServiceMonitor %s: %w", c.Name, err)
	}
	return nil
}
