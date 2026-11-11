package controller

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
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

// Without the CRD a cluster that asks for a ServiceMonitor gets one Warning
// Event, not an error: nothing else of the cluster is held back or marked
// not Ready because Prometheus Operator is not installed.
func TestServiceMonitorWithoutTheCRD(t *testing.T) {
	t.Parallel()

	c := smCluster(true, false)
	r := smReconciler(t, false, c)
	rec := record.NewFakeRecorder(10)
	r.Recorder = rec
	for range 3 {
		if err := r.reconcileServiceMonitor(context.Background(), c); err != nil {
			t.Fatalf("serviceMonitor set without the CRD: error = %v, want none", err)
		}
	}
	if got := len(rec.Events); got != 1 {
		t.Fatalf("3 passes without the CRD: %d Events, want 1", got)
	}
	if ev := <-rec.Events; !strings.Contains(ev, "Warning ServiceMonitorUnavailable") || !strings.Contains(ev, "monitoring.coreos.com") {
		t.Fatalf("Event = %q, want a ServiceMonitorUnavailable Warning naming monitoring.coreos.com", ev)
	}

	off := smCluster(false, false)
	r = smReconciler(t, false, off)
	rec = record.NewFakeRecorder(10)
	r.Recorder = rec
	if err := r.reconcileServiceMonitor(context.Background(), off); err != nil {
		t.Fatalf("serviceMonitor not set, no CRD: error = %v, want none", err)
	}
	if len(rec.Events) != 0 {
		t.Fatal("serviceMonitor not set, no CRD: want no Event")
	}
}

// The CRD lookup misses on every pass while the CRD is absent, and each
// miss is a request to the API server. It is asked again only after
// serviceMonitorRecheck; then a CRD installed meanwhile is found.
func TestServiceMonitorCRDCheckedAgainLater(t *testing.T) {
	t.Parallel()

	c := smCluster(true, false)
	now := time.Unix(1000, 0)
	r := smReconciler(t, false, c)
	lookups := 0
	mapper := &countingMapper{RESTMapper: r.RESTMapper(), lookups: &lookups}
	r.Client = &mapperClient{Client: r.Client, mapper: mapper}
	r.clock = func() time.Time { return now }
	ctx := context.Background()

	for range 3 {
		if err := r.reconcileServiceMonitor(ctx, c); err != nil {
			t.Fatal(err)
		}
	}
	if lookups != 1 {
		t.Fatalf("3 passes within serviceMonitorRecheck: %d lookups, want 1", lookups)
	}

	// The CRD is installed; after serviceMonitorRecheck it is found.
	mapper.RESTMapper.(*meta.DefaultRESTMapper).Add(serviceMonitorGVK, meta.RESTScopeNamespace)
	now = now.Add(serviceMonitorRecheck)
	if err := r.reconcileServiceMonitor(ctx, c); err != nil {
		t.Fatal(err)
	}
	if getServiceMonitor(t, r, c) == nil {
		t.Fatal("CRD installed, recheck due: no ServiceMonitor")
	}
}

// Labels and spec fields the operator does not set, such as the label a
// Prometheus selects ServiceMonitors by or a scrape interval, are kept.
func TestServiceMonitorKeepsOtherFields(t *testing.T) {
	t.Parallel()
	c := smCluster(true, false)
	r := smReconciler(t, true, c)
	ctx := context.Background()
	if err := r.reconcileServiceMonitor(ctx, c); err != nil {
		t.Fatal(err)
	}
	sm := getServiceMonitor(t, r, c)
	l := sm.GetLabels()
	l["release"] = "kube-prometheus-stack"
	sm.SetLabels(l)
	if err := unstructured.SetNestedField(sm.Object, "app", "spec", "jobLabel"); err != nil {
		t.Fatal(err)
	}
	if err := r.Update(ctx, sm); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcileServiceMonitor(ctx, c); err != nil {
		t.Fatal(err)
	}
	sm = getServiceMonitor(t, r, c)
	if sm.GetLabels()["release"] != "kube-prometheus-stack" {
		t.Fatalf("labels = %v, want release kept", sm.GetLabels())
	}
	if job, _, _ := unstructured.NestedString(sm.Object, "spec", "jobLabel"); job != "app" {
		t.Fatalf("spec.jobLabel = %q, want it kept", job)
	}
}

type countingMapper struct {
	meta.RESTMapper
	lookups *int
}

func (m *countingMapper) RESTMapping(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
	if gk == serviceMonitorGVK.GroupKind() {
		*m.lookups++
	}
	return m.RESTMapper.RESTMapping(gk, versions...)
}

type mapperClient struct {
	client.Client
	mapper meta.RESTMapper
}

func (c *mapperClient) RESTMapper() meta.RESTMapper { return c.mapper }

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
	sm.Object["spec"] = map[string]interface{}{"endpoints": []interface{}{}, "selector": map[string]interface{}{}}
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

// countingClient counts the reads of ServiceMonitors.
type countingClient struct {
	client.Client
	gets *int
}

func (c *countingClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if u, ok := obj.(*unstructured.Unstructured); ok && u.GroupVersionKind() == serviceMonitorGVK {
		*c.gets++
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

// With the CRD installed, a cluster that does not ask for a ServiceMonitor
// looks for one of its own once, not on every pass; asking for one and then
// not again looks once more, and deletes it.
func TestServiceMonitorOffLooksOnce(t *testing.T) {
	t.Parallel()
	c := smCluster(false, false)
	r := smReconciler(t, true, c)
	gets := 0
	r.Client = &countingClient{Client: r.Client, gets: &gets}
	ctx := context.Background()
	for range 3 {
		if err := r.reconcileServiceMonitor(ctx, c); err != nil {
			t.Fatal(err)
		}
	}
	if gets != 1 {
		t.Fatalf("3 passes with serviceMonitor off: %d reads, want 1", gets)
	}

	c.Spec.Monitoring.ServiceMonitor = true
	if err := r.reconcileServiceMonitor(ctx, c); err != nil {
		t.Fatal(err)
	}
	c.Spec.Monitoring.ServiceMonitor = false
	gets = 0
	for range 3 {
		if err := r.reconcileServiceMonitor(ctx, c); err != nil {
			t.Fatal(err)
		}
	}
	if gets != 1 {
		t.Fatalf("3 passes after turning it off: %d reads, want 1", gets)
	}
	if getServiceMonitor(t, r, c) != nil {
		t.Fatal("turned off: the ServiceMonitor is still there")
	}
}

// notFoundClient answers every ServiceMonitor request with NotFound, as an
// API server does for a resource whose CRD was deleted.
type notFoundClient struct{ client.Client }

func (c *notFoundClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	return apierrors.NewNotFound(schema.GroupResource{Group: "monitoring.coreos.com", Resource: "servicemonitors"}, key.Name)
}

func (c *notFoundClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	return apierrors.NewNotFound(schema.GroupResource{Group: "monitoring.coreos.com", Resource: "servicemonitors"}, obj.GetName())
}

// Fields added to an endpoint, such as its scrape interval, are kept for
// as long as the endpoint is: the operator sets only its port and path.
func TestServiceMonitorKeepsEndpointFields(t *testing.T) {
	t.Parallel()
	c := smCluster(true, false)
	r := smReconciler(t, true, c)
	ctx := context.Background()
	if err := r.reconcileServiceMonitor(ctx, c); err != nil {
		t.Fatal(err)
	}
	sm := getServiceMonitor(t, r, c)
	if err := unstructured.SetNestedSlice(sm.Object, []interface{}{
		map[string]interface{}{"port": AgentPortName, "path": "/stats", "interval": "30s", "scrapeTimeout": "10s"},
	}, "spec", "endpoints"); err != nil {
		t.Fatal(err)
	}
	if err := r.Update(ctx, sm); err != nil {
		t.Fatal(err)
	}
	c.Spec.Monitoring.Enabled = true
	if err := r.reconcileServiceMonitor(ctx, c); err != nil {
		t.Fatal(err)
	}
	eps, _, _ := unstructured.NestedSlice(getServiceMonitor(t, r, c).Object, "spec", "endpoints")
	want := []interface{}{
		map[string]interface{}{"port": AgentPortName, "path": "/metrics", "interval": "30s", "scrapeTimeout": "10s"},
		map[string]interface{}{"port": ExporterPortName, "path": "/metrics"},
	}
	if !reflect.DeepEqual(eps, want) {
		t.Fatalf("endpoints = %v, want %v", eps, want)
	}
}

// A CRD removed while the operator runs is treated as a CRD not installed:
// one Warning Event, no error, and a new look after serviceMonitorRecheck.
func TestServiceMonitorCRDRemovedWarnsOnce(t *testing.T) {
	t.Parallel()
	c := smCluster(true, false)
	now := time.Unix(1000, 0)
	r := smReconciler(t, true, c)
	r.clock = func() time.Time { return now }
	r.Client = &notFoundClient{Client: r.Client}
	rec := record.NewFakeRecorder(10)
	r.Recorder = rec
	// The mapper still has the kind after serviceMonitorRecheck, so every
	// minute the create is tried again and fails the same way.
	for range 3 {
		for range 3 {
			if err := r.reconcileServiceMonitor(context.Background(), c); err != nil {
				t.Fatalf("CRD removed: error = %v, want none", err)
			}
		}
		now = now.Add(serviceMonitorRecheck)
	}
	if got := len(rec.Events); got != 1 {
		t.Fatalf("3 minutes after the CRD was removed: %d Events, want 1", got)
	}
}
