package controller

import (
	"context"
	"reflect"
	"strings"
	"testing"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// smReconciler returns a reconciler whose API server serves the
// ServiceMonitor kind only if crd is true.
func smReconciler(t *testing.T, crd bool, objs ...client.Object) *KividbClusterReconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := kividbv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	mapper := meta.NewDefaultRESTMapper(nil)
	mapper.Add(kividbv1alpha1.GroupVersion.WithKind("KividbCluster"), meta.RESTScopeNamespace)
	if crd {
		mapper.Add(serviceMonitorGVK, meta.RESTScopeNamespace)
	}
	return &KividbClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithRESTMapper(mapper).WithObjects(objs...).Build(),
		Scheme: scheme,
	}
}

func smCluster(serviceMonitor, exporter bool) *kividbv1alpha1.KividbCluster {
	c := testCluster("demo")
	c.UID = types.UID("demo-uid")
	c.Spec.Monitoring = kividbv1alpha1.MonitoringSpec{Enabled: exporter, ServiceMonitor: serviceMonitor}
	return c
}

func getServiceMonitor(t *testing.T, r *KividbClusterReconciler, c *kividbv1alpha1.KividbCluster) *unstructured.Unstructured {
	t.Helper()
	sm := &unstructured.Unstructured{}
	sm.SetGroupVersionKind(serviceMonitorGVK)
	err := r.Get(context.Background(), client.ObjectKey{Namespace: c.Namespace, Name: c.Name}, sm)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return sm
}

func endpointPorts(t *testing.T, sm *unstructured.Unstructured) []string {
	t.Helper()
	eps, _, err := unstructured.NestedSlice(sm.Object, "spec", "endpoints")
	if err != nil {
		t.Fatal(err)
	}
	var ports []string
	for _, e := range eps {
		ep := e.(map[string]interface{})
		if ep["path"] != "/metrics" {
			t.Fatalf("endpoint %v: want path /metrics", ep)
		}
		ports = append(ports, ep["port"].(string))
	}
	return ports
}

// foreignServiceMonitor is a ServiceMonitor with the cluster's name that the
// cluster does not own.
func foreignServiceMonitor(c *kividbv1alpha1.KividbCluster) *unstructured.Unstructured {
	sm := &unstructured.Unstructured{}
	sm.SetGroupVersionKind(serviceMonitorGVK)
	sm.SetNamespace(c.Namespace)
	sm.SetName(c.Name)
	sm.Object["spec"] = map[string]interface{}{"jobLabel": "someone-elses"}
	return sm
}

func TestServiceMonitorCreated(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name      string
		exporter  bool
		wantPorts []string
	}{
		{name: "agent only", exporter: false, wantPorts: []string{AgentPortName}},
		{name: "agent and redis_exporter", exporter: true, wantPorts: []string{AgentPortName, ExporterPortName}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := smCluster(true, tt.exporter)
			r := smReconciler(t, true, c)
			if err := r.reconcileServiceMonitor(context.Background(), c); err != nil {
				t.Fatal(err)
			}
			sm := getServiceMonitor(t, r, c)
			if sm == nil {
				t.Fatal("no ServiceMonitor created")
			}
			if !metav1.IsControlledBy(sm, c) {
				t.Fatalf("ServiceMonitor owners = %v, want the cluster as controller", sm.GetOwnerReferences())
			}
			if got := endpointPorts(t, sm); !reflect.DeepEqual(got, tt.wantPorts) {
				t.Fatalf("endpoint ports = %v, want %v", got, tt.wantPorts)
			}
		})
	}
}

// The selector picks the cluster's headless Services, replicated or one per
// shard, which carry every pod once; not the master, replica or per-pod ones,
// and not another cluster's.
func TestServiceMonitorSelectsOnlyTheHeadlessServices(t *testing.T) {
	t.Parallel()
	c := smCluster(true, true)
	r := smReconciler(t, true, c)
	if err := r.reconcileServiceMonitor(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	sm := getServiceMonitor(t, r, c)
	matchLabels, _, err := unstructured.NestedStringMap(sm.Object, "spec", "selector", "matchLabels")
	if err != nil || len(matchLabels) == 0 {
		t.Fatalf("selector.matchLabels = %v (%v)", matchLabels, err)
	}
	sel := labels.SelectorFromSet(matchLabels)

	sharded := smCluster(true, true)
	shards := int32(2)
	sharded.Spec.Shards = &shards
	other := smCluster(true, true)
	other.Name = "other"

	want := []*corev1.Service{desiredHeadlessService(c, nil)}
	var notWanted []*corev1.Service
	for _, s := range desiredShardedServices(sharded, nil) {
		if s.Labels[ServiceKindLabel] == "headless" {
			want = append(want, s)
		} else {
			notWanted = append(notWanted, s)
		}
	}
	notWanted = append(notWanted, desiredMasterService(c, nil), desiredReplicaService(c, nil), desiredHeadlessService(other, nil))
	if len(want) != 3 {
		t.Fatalf("want the replicated headless Service and one per shard, got %d", len(want))
	}
	for _, s := range want {
		if !sel.Matches(labels.Set(s.Labels)) {
			t.Errorf("selector %v misses %s (%v)", sel, s.Name, s.Labels)
		}
	}
	for _, s := range notWanted {
		if sel.Matches(labels.Set(s.Labels)) {
			t.Errorf("selector %v also picks %s (%v)", sel, s.Name, s.Labels)
		}
	}
}

func TestServiceMonitorWithoutTheCRD(t *testing.T) {
	t.Parallel()

	c := smCluster(true, false)
	err := smReconciler(t, false, c).reconcileServiceMonitor(context.Background(), c)
	if err == nil || !strings.Contains(err.Error(), "monitoring.coreos.com") {
		t.Fatalf("serviceMonitor set without the CRD: error = %v, want one naming monitoring.coreos.com", err)
	}

	off := smCluster(false, false)
	if err := smReconciler(t, false, off).reconcileServiceMonitor(context.Background(), off); err != nil {
		t.Fatalf("serviceMonitor not set, no CRD: error = %v, want none", err)
	}
}

func TestServiceMonitorDeletedWhenTurnedOff(t *testing.T) {
	t.Parallel()
	c := smCluster(true, false)
	r := smReconciler(t, true, c)
	if err := r.reconcileServiceMonitor(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	c.Spec.Monitoring.ServiceMonitor = false
	if err := r.reconcileServiceMonitor(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	if sm := getServiceMonitor(t, r, c); sm != nil {
		t.Fatal("serviceMonitor turned off, but the cluster's ServiceMonitor is still there")
	}
}

// A ServiceMonitor with the cluster's name that the cluster does not own is
// neither taken over nor deleted.
func TestServiceMonitorLeavesAnotherOwnersAlone(t *testing.T) {
	t.Parallel()

	on := smCluster(true, false)
	r := smReconciler(t, true, on, foreignServiceMonitor(on))
	err := r.reconcileServiceMonitor(context.Background(), on)
	if err == nil || !strings.Contains(err.Error(), "not owned") {
		t.Fatalf("error = %v, want one saying the ServiceMonitor is not owned by the cluster", err)
	}
	sm := getServiceMonitor(t, r, on)
	if len(sm.GetOwnerReferences()) != 0 {
		t.Fatalf("the other ServiceMonitor was taken over: owners %v", sm.GetOwnerReferences())
	}
	if job, _, _ := unstructured.NestedString(sm.Object, "spec", "jobLabel"); job != "someone-elses" {
		t.Fatalf("the other ServiceMonitor was changed: %v", sm.Object["spec"])
	}

	off := smCluster(false, false)
	r = smReconciler(t, true, off, foreignServiceMonitor(off))
	if err := r.reconcileServiceMonitor(context.Background(), off); err != nil {
		t.Fatal(err)
	}
	if getServiceMonitor(t, r, off) == nil {
		t.Fatal("serviceMonitor off: a ServiceMonitor the cluster does not own was deleted")
	}
}

// A ServiceMonitor of the cluster that was edited is put back.
func TestServiceMonitorRestored(t *testing.T) {
	t.Parallel()
	c := smCluster(true, false)
	r := smReconciler(t, true, c)
	ctx := context.Background()
	if err := r.reconcileServiceMonitor(ctx, c); err != nil {
		t.Fatal(err)
	}
	sm := getServiceMonitor(t, r, c)
	want := sm.Object["spec"]
	sm.Object["spec"] = map[string]interface{}{"endpoints": []interface{}{}}
	if err := r.Update(ctx, sm); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcileServiceMonitor(ctx, c); err != nil {
		t.Fatal(err)
	}
	if got := getServiceMonitor(t, r, c).Object["spec"]; !reflect.DeepEqual(got, want) {
		t.Fatalf("spec = %v, want it restored to %v", got, want)
	}
}
