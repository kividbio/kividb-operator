package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	"github.com/kividbio/kividb-operator/internal/agentapi"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// fakeAgents stands in for every pod's agent sidecar, keyed by pod IP. It
// applies /promote and /replicaof to its own state the way kividb would,
// so a test can run several reconcile passes and watch them converge.
type fakeAgents struct {
	status map[string]*agentapi.StatusResponse
	calls  []string
}

func (f *fakeAgents) RoundTrip(req *http.Request) (*http.Response, error) {
	ip, _, err := net.SplitHostPort(req.URL.Host)
	if err != nil {
		return nil, err
	}
	st, ok := f.status[ip]
	if !ok {
		return nil, fmt.Errorf("dial %s: connection refused", ip)
	}

	var body any = agentapi.OKResponse{OK: true}
	switch req.URL.Path {
	case "/status":
		body = st
	case "/promote":
		f.calls = append(f.calls, "promote "+ip)
		st.Role, st.MasterHost, st.MasterPort = agentapi.RoleMaster, "", 0
	case "/replicaof":
		var r agentapi.ReplicaOfRequest
		if err := json.NewDecoder(req.Body).Decode(&r); err != nil {
			return nil, err
		}
		f.calls = append(f.calls, fmt.Sprintf("replicaof %s -> %s", ip, r.Host))
		st.Role, st.MasterHost, st.MasterPort = agentapi.RoleReplica, r.Host, r.Port
	}
	b, _ := json.Marshal(body)
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(b)), Header: http.Header{}}, nil
}

type testPod struct {
	name, ip, label string
	ready           bool
	unreadyFor      time.Duration
	agent           *agentapi.StatusResponse // nil: agent unreachable
}

func master(offset int64) *agentapi.StatusResponse {
	return &agentapi.StatusResponse{Role: agentapi.RoleMaster, ReplicationOffset: offset}
}

func replicaOf(host string, offset int64) *agentapi.StatusResponse {
	return &agentapi.StatusResponse{Role: agentapi.RoleReplica, MasterHost: host, MasterPort: 6380, ReplicationOffset: offset}
}

type rolesHarness struct {
	t       *testing.T
	r       *KividbClusterReconciler
	cluster *kividbv1alpha1.KividbCluster
	agents  *fakeAgents
}

func newRolesHarness(t *testing.T, statusMaster string, pods ...testPod) *rolesHarness {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	threshold := int32(20)
	c := &kividbv1alpha1.KividbCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "default"},
		Spec:       kividbv1alpha1.KividbClusterSpec{Replicas: int32(len(pods) - 1)},
		Status:     kividbv1alpha1.KividbClusterStatus{MasterPod: statusMaster},
	}
	c.Spec.Failover.UnhealthyThresholdSeconds = &threshold

	agents := &fakeAgents{status: map[string]*agentapi.StatusResponse{}}
	var objs []client.Object
	for _, p := range pods {
		labels := selectorLabels(c)
		if p.label != "" {
			labels[kividbv1alpha1.RoleLabel] = p.label
		}
		cond := corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue}
		if !p.ready {
			cond.Status = corev1.ConditionFalse
			cond.LastTransitionTime = metav1.NewTime(time.Now().Add(-p.unreadyFor))
		}
		objs = append(objs, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: p.name, Namespace: c.Namespace, Labels: labels},
			Status:     corev1.PodStatus{PodIP: p.ip, Conditions: []corev1.PodCondition{cond}},
		})
		if p.agent != nil {
			agents.status[p.ip] = p.agent
		}
	}

	return &rolesHarness{
		t:       t,
		cluster: c,
		agents:  agents,
		r: &KividbClusterReconciler{
			Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build(),
			Scheme: scheme,
			Agent:  &AgentClient{http: &http.Client{Transport: agents}},
		},
	}
}

// reconcile runs one reconcileRoles pass against the pods as they currently
// are in the fake API server, and persists status.masterPod the way
// updateStatus would on success.
func (h *rolesHarness) reconcile() (masterPod string, failover bool) {
	h.t.Helper()
	var list corev1.PodList
	if err := h.r.List(context.Background(), &list, client.InNamespace(h.cluster.Namespace)); err != nil {
		h.t.Fatal(err)
	}
	_, masterPod, failover, err := h.r.reconcileRoles(context.Background(), h.cluster, list.Items)
	if err != nil {
		h.t.Fatalf("reconcileRoles: %v", err)
	}
	h.cluster.Status.MasterPod = masterPod
	return masterPod, failover
}

func (h *rolesHarness) setReady(name string, ready bool) {
	h.t.Helper()
	var p corev1.Pod
	key := types.NamespacedName{Namespace: h.cluster.Namespace, Name: name}
	if err := h.r.Get(context.Background(), key, &p); err != nil {
		h.t.Fatal(err)
	}
	p.Status.Conditions[0].Status = corev1.ConditionFalse
	if ready {
		p.Status.Conditions[0].Status = corev1.ConditionTrue
	}
	if err := h.r.Status().Update(context.Background(), &p); err != nil {
		h.t.Fatal(err)
	}
}

func (h *rolesHarness) assertLabels(want map[string]string) {
	h.t.Helper()
	for name, role := range want {
		var p corev1.Pod
		if err := h.r.Get(context.Background(), types.NamespacedName{Namespace: h.cluster.Namespace, Name: name}, &p); err != nil {
			h.t.Fatal(err)
		}
		if got := p.Labels[kividbv1alpha1.RoleLabel]; got != role {
			h.t.Errorf("pod %s: role label = %q, want %q", name, got, role)
		}
	}
}

func (h *rolesHarness) assertRole(ip string, role agentapi.Role, masterHost string) {
	h.t.Helper()
	st := h.agents.status[ip]
	if st.Role != role || st.MasterHost != masterHost {
		h.t.Errorf("kividb at %s: role=%s masterHost=%q, want role=%s masterHost=%q", ip, st.Role, st.MasterHost, role, masterHost)
	}
}

// A hung master is failed over, then comes back still believing it is a
// master. It must rejoin as a replica of the pod that replaced it -- not
// be handed the cluster back, which would discard everything written to
// the new master in the meantime.
func TestReconcileRoles_ReturningOldMasterDoesNotReclaim(t *testing.T) {
	h := newRolesHarness(t, "c1-0",
		testPod{name: "c1-0", ip: "10.0.0.1", label: "master", unreadyFor: time.Minute, agent: master(100)},
		testPod{name: "c1-1", ip: "10.0.0.2", label: "replica", ready: true, agent: replicaOf("10.0.0.1", 100)},
		testPod{name: "c1-2", ip: "10.0.0.3", label: "replica", ready: true, agent: replicaOf("10.0.0.1", 90)},
	)

	if m, failover := h.reconcile(); m != "c1-1" || !failover {
		t.Fatalf("pass 1: master=%q failover=%v, want c1-1 with a failover", m, failover)
	}
	h.assertLabels(map[string]string{"c1-0": "replica", "c1-1": "master", "c1-2": "replica"})

	// While the old master is still down, nothing more should happen.
	if m, failover := h.reconcile(); m != "c1-1" || failover {
		t.Fatalf("pass 2: master=%q failover=%v, want c1-1 and no repeated failover", m, failover)
	}

	h.setReady("c1-0", true)
	if m, failover := h.reconcile(); m != "c1-1" || failover {
		t.Fatalf("pass 3: master=%q failover=%v, want c1-1 to stay master", m, failover)
	}
	h.assertLabels(map[string]string{"c1-0": "replica", "c1-1": "master", "c1-2": "replica"})
	h.assertRole("10.0.0.2", agentapi.RoleMaster, "")
	h.assertRole("10.0.0.1", agentapi.RoleReplica, "10.0.0.2")
	h.assertRole("10.0.0.3", agentapi.RoleReplica, "10.0.0.2")
}

// Clusters that failed over under an older operator can have the master
// label on both the old and the new master. The persisted status.masterPod
// decides, and the loser is relabeled.
func TestReconcileRoles_TwoLabeledMasters(t *testing.T) {
	h := newRolesHarness(t, "c1-1",
		testPod{name: "c1-0", ip: "10.0.0.1", label: "master", ready: true, agent: master(100)},
		testPod{name: "c1-1", ip: "10.0.0.2", label: "master", ready: true, agent: master(140)},
		testPod{name: "c1-2", ip: "10.0.0.3", label: "replica", ready: true, agent: replicaOf("10.0.0.2", 140)},
	)

	if m, failover := h.reconcile(); m != "c1-1" || failover {
		t.Fatalf("master=%q failover=%v, want c1-1 and no failover", m, failover)
	}
	h.assertLabels(map[string]string{"c1-0": "replica", "c1-1": "master", "c1-2": "replica"})
	h.assertRole("10.0.0.1", agentapi.RoleReplica, "10.0.0.2")
}

// The labeled master was turned into a replica of another pod that is a
// healthy master (a manual switchover): follow it.
func TestReconcileRoles_AdoptsManuallyPromotedMaster(t *testing.T) {
	h := newRolesHarness(t, "c1-0",
		testPod{name: "c1-0", ip: "10.0.0.1", label: "master", ready: true, agent: replicaOf("10.0.0.2", 100)},
		testPod{name: "c1-1", ip: "10.0.0.2", label: "replica", ready: true, agent: master(100)},
		testPod{name: "c1-2", ip: "10.0.0.3", label: "replica", ready: true, agent: replicaOf("10.0.0.1", 100)},
	)

	if m, _ := h.reconcile(); m != "c1-1" {
		t.Fatalf("master=%q, want c1-1", m)
	}
	h.assertLabels(map[string]string{"c1-0": "replica", "c1-1": "master", "c1-2": "replica"})
	h.assertRole("10.0.0.2", agentapi.RoleMaster, "")
	h.assertRole("10.0.0.1", agentapi.RoleReplica, "10.0.0.2")
	h.assertRole("10.0.0.3", agentapi.RoleReplica, "10.0.0.2")
}

// Every pod is a replica and two of them replicate from each other: there
// is no master to adopt, so the labeled one is promoted again.
func TestReconcileRoles_RepairsReplicationCycle(t *testing.T) {
	h := newRolesHarness(t, "c1-0",
		testPod{name: "c1-0", ip: "10.0.0.1", label: "master", ready: true, agent: replicaOf("10.0.0.2", 62)},
		testPod{name: "c1-1", ip: "10.0.0.2", label: "replica", ready: true, agent: replicaOf("10.0.0.1", 62)},
		testPod{name: "c1-2", ip: "10.0.0.3", label: "replica", ready: true, agent: replicaOf("10.0.0.1", 62)},
	)

	if m, _ := h.reconcile(); m != "c1-0" {
		t.Fatalf("master=%q, want c1-0", m)
	}
	h.assertLabels(map[string]string{"c1-0": "master", "c1-1": "replica", "c1-2": "replica"})
	h.assertRole("10.0.0.1", agentapi.RoleMaster, "")
	h.assertRole("10.0.0.2", agentapi.RoleReplica, "10.0.0.1")
	h.assertRole("10.0.0.3", agentapi.RoleReplica, "10.0.0.1")
}

// A cluster bootstrapped from a snapshot must elect the pod that holds the
// restored data, even though the empty pods are Ready first.
func TestReconcileRoles_BootstrapElectsSeededPod(t *testing.T) {
	h := newRolesHarness(t, "",
		testPod{name: "c1-0", ip: "10.0.0.1", unreadyFor: time.Second, agent: master(0)},
		testPod{name: "c1-1", ip: "10.0.0.2", ready: true, agent: master(0)},
	)
	h.cluster.Spec.BootstrapFromSnapshot = &kividbv1alpha1.BootstrapFromSnapshotSpec{}

	var list corev1.PodList
	if err := h.r.List(context.Background(), &list, client.InNamespace(h.cluster.Namespace)); err != nil {
		t.Fatal(err)
	}
	if _, m, _, err := h.r.reconcileRoles(context.Background(), h.cluster, list.Items); err == nil {
		t.Fatalf("elected %q while the seeded pod was not ready, want an error", m)
	}
	if len(h.agents.calls) != 0 {
		t.Fatalf("unexpected agent calls before the seeded pod is ready: %v", h.agents.calls)
	}

	h.setReady("c1-0", true)
	if m, _ := h.reconcile(); m != "c1-0" {
		t.Fatalf("master=%q, want the seeded pod c1-0", m)
	}
	h.assertLabels(map[string]string{"c1-0": "master", "c1-1": "replica"})
	h.assertRole("10.0.0.2", agentapi.RoleReplica, "10.0.0.1")
}

// A master that has been unready for less than the threshold is left alone.
func TestReconcileRoles_NoFailoverBeforeThreshold(t *testing.T) {
	h := newRolesHarness(t, "c1-0",
		testPod{name: "c1-0", ip: "10.0.0.1", label: "master", unreadyFor: 5 * time.Second, agent: master(100)},
		testPod{name: "c1-1", ip: "10.0.0.2", label: "replica", ready: true, agent: replicaOf("10.0.0.1", 100)},
	)

	if m, failover := h.reconcile(); m != "c1-0" || failover {
		t.Fatalf("master=%q failover=%v, want c1-0 and no failover", m, failover)
	}
	h.assertLabels(map[string]string{"c1-0": "master", "c1-1": "replica"})
	if len(h.agents.calls) != 0 {
		t.Errorf("unexpected agent calls: %v", h.agents.calls)
	}
}
