package controller

import (
	"context"
	"fmt"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
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

// serviceMonitorRecheck is how long the operator goes without asking the
// API server again for the ServiceMonitor kind after it was not served.
// Each lookup of a kind not served is a request, and every cluster asks on
// every pass.
const serviceMonitorRecheck = time.Minute

// serviceMonitorEndpoints scrapes /metrics on the agent's port of every
// pod, and on the redis_exporter's with spec.monitoring.enabled.
func serviceMonitorEndpoints(c *kividbv1alpha1.KividbCluster) []interface{} {
	endpoints := []interface{}{
		map[string]interface{}{"port": AgentPortName, "path": "/metrics"},
	}
	if c.Spec.Monitoring.Enabled {
		endpoints = append(endpoints, map[string]interface{}{"port": ExporterPortName, "path": "/metrics"})
	}
	return endpoints
}

// serviceMonitorSelector picks the cluster's headless Services: the
// replicated cluster's one, or a sharded cluster's one per shard. Each has
// every pod of the cluster (or shard) once.
func serviceMonitorSelector(c *kividbv1alpha1.KividbCluster) map[string]interface{} {
	matchLabels := map[string]interface{}{ServiceKindLabel: "headless"}
	for k, v := range selectorLabels(c) {
		matchLabels[k] = v
	}
	return map[string]interface{}{"matchLabels": matchLabels}
}

// serviceMonitorServed reports whether the API server serves the
// ServiceMonitor kind. After a miss it answers false without asking again
// for serviceMonitorRecheck.
func (r *KividbClusterReconciler) serviceMonitorServed() (bool, error) {
	if until, ok := r.serviceMonitorUnserved.Load().(time.Time); ok && r.now().Before(until) {
		return false, nil
	}
	_, err := r.RESTMapper().RESTMapping(serviceMonitorGVK.GroupKind(), serviceMonitorGVK.Version)
	if meta.IsNoMatchError(err) {
		r.serviceMonitorUnserved.Store(r.now().Add(serviceMonitorRecheck))
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("looking up the ServiceMonitor kind: %w", err)
	}
	r.serviceMonitorUnserved.Store(time.Time{})
	return true, nil
}

// reconcileServiceMonitor makes the cluster's ServiceMonitor, named after
// it, when spec.monitoring.serviceMonitor is set, and deletes it when not.
// It sets the selector, the endpoints and its own labels; other labels and
// spec fields, such as the label a Prometheus selects by, are kept. A
// ServiceMonitor of that name the cluster does not own is left alone: it
// is refused when the cluster wants one, and kept when it does not.
//
// Without the Prometheus Operator's CRD there is nothing to make: a
// cluster that asks for one gets a Warning Event, once, and nothing else
// of the cluster is held back for it.
func (r *KividbClusterReconciler) reconcileServiceMonitor(ctx context.Context, c *kividbv1alpha1.KividbCluster) error {
	want := c.Spec.Monitoring.ServiceMonitor
	served, err := r.serviceMonitorServed()
	if err != nil {
		return err
	}
	key := c.Namespace + "/" + c.Name
	if !served {
		if !want {
			r.serviceMonitorWarned.Delete(key)
			return nil
		}
		if _, warned := r.serviceMonitorWarned.LoadOrStore(key, struct{}{}); !warned {
			r.event(c, corev1.EventTypeWarning, "ServiceMonitorUnavailable",
				"spec.monitoring.serviceMonitor is set, but the API server does not serve servicemonitors.monitoring.coreos.com: install the Prometheus Operator's CRDs")
		}
		return nil
	}
	r.serviceMonitorWarned.Delete(key)

	sm := &unstructured.Unstructured{}
	sm.SetGroupVersionKind(serviceMonitorGVK)
	sm.SetNamespace(c.Namespace)
	sm.SetName(c.Name)
	if !want {
		err := r.Get(ctx, client.ObjectKeyFromObject(sm), sm)
		if apierrors.IsNotFound(err) || (err == nil && !metav1.IsControlledBy(sm, c)) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("reading ServiceMonitor %s: %w", c.Name, err)
		}
		if err := r.Delete(ctx, sm); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("deleting ServiceMonitor %s: %w", c.Name, err)
		}
		return nil
	}

	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, sm, func() error {
		if sm.GetResourceVersion() != "" && !metav1.IsControlledBy(sm, c) {
			return fmt.Errorf("already exists and is not owned by this cluster")
		}
		labels := sm.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		for k, v := range commonLabels(c) {
			labels[k] = v
		}
		sm.SetLabels(labels)
		if err := unstructured.SetNestedField(sm.Object, serviceMonitorSelector(c), "spec", "selector"); err != nil {
			return err
		}
		if err := unstructured.SetNestedSlice(sm.Object, serviceMonitorEndpoints(c), "spec", "endpoints"); err != nil {
			return err
		}
		return controllerutil.SetControllerReference(c, sm, r.scheme())
	})
	if apierrors.IsNotFound(err) {
		// The kind was served when last looked up; its CRD has since gone.
		return fmt.Errorf("ServiceMonitor %s: %w (was the servicemonitors.monitoring.coreos.com CRD removed?)", c.Name, err)
	}
	if err != nil {
		return fmt.Errorf("ServiceMonitor %s: %w", c.Name, err)
	}
	return nil
}
