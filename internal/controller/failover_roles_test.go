package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	"github.com/kividbio/kividb-operator/internal/agentapi"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// fakeAgents stands in for every pod's agent sidecar, keyed by pod IP. It
// applies /promote and /replicaof to its own state the way kividb would,
// so a test can run several reconcile passes and watch them converge.
type fakeAgents struct {
	status map[string]*agentapi.StatusResponse
	calls  []string

	// paused: the pods whose writes CLIENT PAUSE holds.
	paused map[string]bool
	// catchUp: how many ROLE answers a replica takes to reach its master's
	// offset (it reports its own status offset until then); -1 never.
	catchUp map[string]int
	// noPause: the engine refuses CLIENT PAUSE ... WRITE (before 1.1).
	noPause bool
	// failReplicaOf: /replicaof fails on these pods.
	failReplicaOf map[string]bool
	// failPromote: /promote fails on these pods.
	failPromote map[string]bool
	// failPromoteAfter: /promote takes effect on these pods, then fails
	// (the reply is lost).
	failPromoteAfter map[string]bool
	// hang: nothing answers on these addresses -- a request waits until
	// its context ends, as one to a dead node does.
	hang map[string]bool
	// pauseUnanswered: the next this many CLIENT PAUSEs on an address take
	// effect, but no reply comes back before the request's context ends.
	pauseUnanswered map[string]int
	// goneAfterPause: once paused, nothing answers on these addresses (the
	// container stopped).
	goneAfterPause map[string]bool
}

// execReply answers /exec the way kividb would, for the commands the
// operator sends: ROLE, CLIENT PAUSE / UNPAUSE / KILL, ACL SETUSER, PING.
func (f *fakeAgents) execReply(ip string, st *agentapi.StatusResponse, args []string) agentapi.ExecResponse {
	ok := agentapi.ExecResponse{Type: "status", Str: "OK"}
	cmd := strings.ToUpper(strings.Join(args, " "))
	if cmd != "ROLE" {
		f.calls = append(f.calls, "exec "+ip+" "+strings.Join(args, " "))
	}
	switch {
	case cmd == "ROLE":
		if st.Role == agentapi.RoleMaster {
			return agentapi.ExecResponse{Type: "array", Array: []agentapi.ExecResponse{
				{Type: "bulk", Str: "master"}, {Type: "integer", Int: st.ReplicationOffset}, {Type: "array"},
			}}
		}
		offset := st.ReplicationOffset
		if m := f.byIP(st.MasterHost); m != nil {
			switch n := f.catchUp[ip]; {
			case n == 0:
				offset = max(offset, m.ReplicationOffset)
			case n > 0:
				f.catchUp[ip] = n - 1
			}
		}
		state := "connected"
		if st.MasterLinkUp != nil && !*st.MasterLinkUp {
			state = "connect"
		}
		return agentapi.ExecResponse{Type: "array", Array: []agentapi.ExecResponse{
			{Type: "bulk", Str: "slave"}, {Type: "bulk", Str: st.MasterHost}, {Type: "integer", Int: int64(st.MasterPort)},
			{Type: "bulk", Str: state}, {Type: "integer", Int: offset},
		}}
	case strings.HasPrefix(cmd, "CLIENT PAUSE"):
		if f.noPause {
			return agentapi.ExecResponse{Type: "error", Str: "ERR syntax error"}
		}
		if f.paused == nil {
			f.paused = map[string]bool{}
		}
		f.paused[ip] = true
	case cmd == "CLIENT UNPAUSE":
		delete(f.paused, ip)
	case cmd == "PING":
		return agentapi.ExecResponse{Type: "status", Str: "PONG"}
	case cmd == "INFO SERVER":
		return agentapi.ExecResponse{Type: "bulk", Str: "# Server" + crlf + "kividb_version:1.2.0" + crlf + "run_id:" + st.RunID + crlf}
	}
	return ok
}

const crlf = "\r\n"

func (f *fakeAgents) byIP(ip string) *agentapi.StatusResponse {
	if ip == "" {
		return nil
	}
	return f.status[ip]
}

func (f *fakeAgents) RoundTrip(req *http.Request) (*http.Response, error) {
	ip, _, err := net.SplitHostPort(req.URL.Host)
	if err != nil {
		return nil, err
	}
	if f.hang[ip] {
		<-req.Context().Done()
		return nil, req.Context().Err()
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
		if f.failPromote[ip] {
			b, _ := json.Marshal(agentapi.ErrorResponse{Error: "connection reset"})
			return &http.Response{StatusCode: http.StatusInternalServerError, Body: io.NopCloser(bytes.NewReader(b)), Header: http.Header{}}, nil
		}
		st.Role, st.MasterHost, st.MasterPort = agentapi.RoleMaster, "", 0
		if f.failPromoteAfter[ip] {
			b, _ := json.Marshal(agentapi.ErrorResponse{Error: "connection reset"})
			return &http.Response{StatusCode: http.StatusInternalServerError, Body: io.NopCloser(bytes.NewReader(b)), Header: http.Header{}}, nil
		}
	case "/replicaof":
		var r agentapi.ReplicaOfRequest
		if err := json.NewDecoder(req.Body).Decode(&r); err != nil {
			return nil, err
		}
		f.calls = append(f.calls, fmt.Sprintf("replicaof %s -> %s", ip, r.Host))
		if f.failReplicaOf[ip] {
			b, _ := json.Marshal(agentapi.ErrorResponse{Error: "connection reset"})
			return &http.Response{StatusCode: http.StatusInternalServerError, Body: io.NopCloser(bytes.NewReader(b)), Header: http.Header{}}, nil
		}
		st.Role, st.MasterHost, st.MasterPort = agentapi.RoleReplica, r.Host, r.Port
	case "/exec":
		var r agentapi.ExecRequest
		if err := json.NewDecoder(req.Body).Decode(&r); err != nil {
			return nil, err
		}
		body = f.execReply(ip, st, r.Args)
		if strings.HasPrefix(strings.ToUpper(strings.Join(r.Args, " ")), "CLIENT PAUSE") {
			if f.goneAfterPause[ip] {
				delete(f.status, ip)
			}
			if f.pauseUnanswered[ip] > 0 {
				f.pauseUnanswered[ip]--
				<-req.Context().Done()
				return nil, req.Context().Err()
			}
		}
	}
	b, _ := json.Marshal(body)
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(b)), Header: http.Header{}}, nil
}

type testPod struct {
	name, ip, label string
	ready           bool
	unreadyFor      time.Duration
	agent           *agentapi.StatusResponse // nil: agent unreachable
	uid             types.UID
	restarts        int32 // of the kividb container
}

// object is p's pod object, in cluster c.
func (p testPod) object(c *kividbv1alpha1.KividbCluster) *corev1.Pod {
	labels := selectorLabels(c)
	if p.label != "" {
		labels[kividbv1alpha1.RoleLabel] = p.label
	}
	cond := corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue}
	if !p.ready {
		cond.Status = corev1.ConditionFalse
		cond.LastTransitionTime = metav1.NewTime(time.Now().Add(-p.unreadyFor))
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: p.name, Namespace: c.Namespace, Labels: labels, UID: p.uid},
		Status: corev1.PodStatus{PodIP: p.ip, Conditions: []corev1.PodCondition{cond},
			ContainerStatuses: []corev1.ContainerStatus{{Name: kividbContainerName, RestartCount: p.restarts}}},
	}
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
	if err := appsv1.AddToScheme(scheme); err != nil {
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
		objs = append(objs, p.object(c))
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

// reconcileAndPersist is reconcile that also keeps the pass's pod
// statuses, as updateStatus would: the next pass sees them as the last
// persisted ones.
func (h *rolesHarness) reconcileAndPersist() (masterPod string, failover bool) {
	h.t.Helper()
	var list corev1.PodList
	if err := h.r.List(context.Background(), &list, client.InNamespace(h.cluster.Namespace)); err != nil {
		h.t.Fatal(err)
	}
	statuses, masterPod, failover, err := h.r.reconcileRoles(context.Background(), h.cluster, list.Items)
	if err != nil {
		h.t.Fatalf("reconcileRoles: %v", err)
	}
	h.cluster.Status.MasterPod, h.cluster.Status.Pods = masterPod, statuses
	return masterPod, failover
}

func (h *rolesHarness) podStatus(name string) kividbv1alpha1.KividbPodStatus {
	h.t.Helper()
	for _, ps := range h.cluster.Status.Pods {
		if ps.Name == name {
			return ps
		}
	}
	h.t.Fatalf("no status for pod %s", name)
	return kividbv1alpha1.KividbPodStatus{}
}

// Each pod's entry records the address its agent last answered on; one
// that cannot be asked keeps the one it had (#53: a failover holds the
// writes of the master it replaces there, once its pod is gone).
func TestReconcileRoles_RecordsEachPodsAddress(t *testing.T) {
	h := newRolesHarness(t, "c1-0",
		testPod{name: "c1-0", ip: "10.0.0.1", label: "master", ready: true, agent: masterRun(100, "s", "run-0")},
		testPod{name: "c1-1", ip: "10.0.0.2", label: "replica", ready: true, agent: replicaOf("10.0.0.1", 100)},
		testPod{name: "c1-2", ip: "10.0.0.3", label: "replica", unreadyFor: time.Second},
	)
	h.cluster.Status.Pods = []kividbv1alpha1.KividbPodStatus{{Name: "c1-2", PodIP: "10.0.0.33", RunID: "run-2"}}

	h.reconcileAndPersist()
	for name, want := range map[string]string{"c1-0": "10.0.0.1", "c1-1": "10.0.0.2", "c1-2": "10.0.0.33"} {
		if got := h.podStatus(name).PodIP; got != want {
			t.Errorf("%s: podIP = %q, want %q", name, got, want)
		}
	}
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

// The master pod was deleted and came back (a new IP) as the master, from
// a snapshot older than its last writes. Its replicas, still following its
// old address, hold those writes: one of them takes over, and the
// restarted pod becomes its replica, rather than every replica being
// resynced from the older data (A2a: 92 acknowledged writes lost).
//
// Killed, the engine starts a new stream (a new run_id, a new replication
// ID, a low offset). Its writes are held from before the promotion until
// it follows the new master.
func TestReconcileRoles_MasterRestartedBehindItsReplicas(t *testing.T) {
	h := newRolesHarness(t, "c1-0",
		testPod{name: "c1-0", ip: "10.0.0.9", ready: true, agent: masterRun(700, "stream-b", "run-2")},
		testPod{name: "c1-1", ip: "10.0.0.2", label: "replica", ready: true, agent: replicaOf("10.0.0.1", 1000)},
		testPod{name: "c1-2", ip: "10.0.0.3", label: "replica", ready: true, agent: replicaOf("10.0.0.1", 990)},
	)
	h.cluster.Status.Pods = []kividbv1alpha1.KividbPodStatus{
		{Name: "c1-0", Role: kividbv1alpha1.RoleMaster, Ready: false, Synced: true, ReplicationOffset: 1000, ReplID: "stream-a", RunID: "run-1"},
		{Name: "c1-1", Role: kividbv1alpha1.RoleReplica, Ready: true, Synced: true, ReplicationOffset: 1000},
		{Name: "c1-2", Role: kividbv1alpha1.RoleReplica, Ready: true, Synced: true, ReplicationOffset: 990},
	}
	if m, failover := h.reconcile(); m != "c1-1" || !failover {
		t.Fatalf("master=%q failover=%v, want a failover to c1-1", m, failover)
	}
	h.assertLabels(map[string]string{"c1-0": "replica", "c1-1": "master", "c1-2": "replica"})
	h.assertRole("10.0.0.9", agentapi.RoleReplica, "10.0.0.2")
	h.assertRole("10.0.0.3", agentapi.RoleReplica, "10.0.0.2")
	h.assertCallOrder(
		"exec 10.0.0.9 CLIENT PAUSE 10000 WRITE",
		"promote 10.0.0.2",
		"replicaof 10.0.0.9 -> 10.0.0.2",
		"exec 10.0.0.9 CLIENT KILL TYPE normal",
		"exec 10.0.0.9 CLIENT UNPAUSE",
	)
	if h.agents.paused["10.0.0.9"] {
		t.Error("the restarted master's writes are still held")
	}
	for _, call := range h.agents.calls {
		if strings.Contains(call, "CLIENT PAUSE 60000") {
			t.Errorf("the restarted master was fenced a second time: %q", call)
		}
	}
}

// The promotion failed, or seemed to: it may have gone through all the
// same, so the restarted master's writes stay held (they end by
// themselves, and the next pass holds them again).
func TestReconcileRoles_RestartedBehindKeepsThePauseWhenThePromotionFails(t *testing.T) {
	h := newRolesHarness(t, "c1-0",
		testPod{name: "c1-0", ip: "10.0.0.9", ready: true, agent: masterRun(700, "stream-b", "run-2")},
		testPod{name: "c1-1", ip: "10.0.0.2", label: "replica", ready: true, agent: replicaOf("10.0.0.1", 1000)},
	)
	h.cluster.Status.Pods = []kividbv1alpha1.KividbPodStatus{
		{Name: "c1-0", Role: kividbv1alpha1.RoleMaster, Ready: false, Synced: true, ReplicationOffset: 1000, ReplID: "stream-a", RunID: "run-1"},
		{Name: "c1-1", Role: kividbv1alpha1.RoleReplica, Ready: true, Synced: true, ReplicationOffset: 1000},
	}
	h.agents.failPromoteAfter = map[string]bool{"10.0.0.2": true}
	if _, _, _, err := h.r.reconcileRoles(context.Background(), h.cluster, h.list()); err == nil {
		t.Fatal("reconcileRoles succeeded, want the promotion's error")
	}
	h.assertCallOrder("exec 10.0.0.9 CLIENT PAUSE 10000 WRITE", "promote 10.0.0.2")
	for _, call := range h.agents.calls {
		if call == "exec 10.0.0.9 CLIENT UNPAUSE" {
			t.Errorf("the restarted master's pause was lifted after the promotion failed (calls %v)", h.agents.calls)
		}
	}
}

// The same with the same stream: a master that went on with its stream,
// after a restart, from below where it was.
func TestReconcileRoles_MasterRestartedBehindInItsOwnStream(t *testing.T) {
	h := newRolesHarness(t, "c1-0",
		testPod{name: "c1-0", ip: "10.0.0.9", ready: true, agent: masterRun(700, "stream-a", "run-2")},
		testPod{name: "c1-1", ip: "10.0.0.2", label: "replica", ready: true, agent: replicaOf("10.0.0.1", 1000)},
	)
	h.cluster.Status.Pods = []kividbv1alpha1.KividbPodStatus{
		{Name: "c1-0", Role: kividbv1alpha1.RoleMaster, Ready: true, Synced: true, ReplicationOffset: 1000, ReplID: "stream-a", RunID: "run-1"},
		{Name: "c1-1", Role: kividbv1alpha1.RoleReplica, Ready: true, Synced: true, ReplicationOffset: 1000},
	}
	if m, failover := h.reconcile(); m != "c1-1" || !failover {
		t.Fatalf("master=%q failover=%v, want a failover to c1-1", m, failover)
	}
}

// With the append-only file on, a killed master reloads it but comes back
// in a new stream. Holding at least as many keys as the best replica, it
// stays the master; with fewer, or either count unknown, it is failed
// away from, as is a master without the append-only file, whatever its
// key count. Equal counts keep the master: nothing says the replica holds
// more. All this only on an engine that acknowledges a write once it is in
// the file (1.2.0 or later, a release candidate too, with
// aof-ack-after-write yes): any other, or one whose agent does not say,
// may have lost acknowledged writes its replica holds, and is failed away
// from. A 1.1.3 engine answers yes for the setting if a client set it.
func TestReconcileRoles_MasterRestartedInANewStreamWithTheAOF(t *testing.T) {
	for _, tc := range []struct {
		name               string
		aof                bool
		version, ack       string
		keys               int64
		keysKnown          bool
		replicaKeysUnknown bool
		wantFailover       bool
	}{
		{"aof, more keys", true, "1.2.0", "yes", 1000, true, false, false},
		{"aof, equal keys", true, "1.2.0", "yes", 900, true, false, false},
		{"aof, fewer keys", true, "1.2.0", "yes", 0, true, false, true},
		{"aof, keys unknown", true, "1.2.0", "yes", 1000, false, false, true},
		{"aof, replica's keys unknown", true, "1.2.0", "yes", 1000, true, true, true},
		{"no aof", false, "1.2.0", "yes", 1000, true, false, true},
		{"aof, equal keys, release candidate", true, "1.2.0-rc7", "yes", 900, true, false, false},
		{"aof, equal keys, later engine", true, "1.10.0", "yes", 900, true, false, false},
		{"aof, equal keys, engine 1.1.3 answering yes", true, "1.1.3", "yes", 900, true, false, true},
		{"aof, more keys, acks before the write", true, "1.2.0", "no", 1000, true, false, true},
		{"aof, more keys, agent reports neither", true, "", "", 1000, true, false, true},
		{"aof, more keys, no version reported", true, "", "yes", 1000, true, false, true},
		{"aof, more keys, no setting reported", true, "1.2.0", "", 1000, true, false, true},
		{"aof, more keys, unknown version", true, "1.2", "yes", 1000, true, false, true},
		{"aof, more keys, garbage version", true, "1.x.0-rc1", "yes", 1000, true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			restarted := keyed(masterRun(0, "stream-b", "run-2"), tc.keys)
			restarted.AofEnabled, restarted.KeyCountKnown = tc.aof, tc.keysKnown
			restarted.EngineVersion, restarted.AofAckAfterWrite = tc.version, tc.ack
			replica := keyed(linkDown(replicaOf("10.0.0.1", 900)), 900)
			replica.KeyCountKnown = !tc.replicaKeysUnknown
			h := newRolesHarness(t, "c1-0",
				testPod{name: "c1-0", ip: "10.0.0.9", ready: true, agent: restarted},
				testPod{name: "c1-1", ip: "10.0.0.2", label: "replica", ready: true, agent: replica},
			)
			h.cluster.Status.Pods = []kividbv1alpha1.KividbPodStatus{
				{Name: "c1-0", Role: kividbv1alpha1.RoleMaster, Ready: true, Synced: true, ReplicationOffset: 1000, ReplID: "stream-a", RunID: "run-1"},
				{Name: "c1-1", Role: kividbv1alpha1.RoleReplica, Ready: true, Synced: true, ReplicationOffset: 900},
			}
			want := "c1-0"
			if tc.wantFailover {
				want = "c1-1"
			}
			if m, failover := h.reconcile(); m != want || failover != tc.wantFailover {
				t.Fatalf("master=%q failover=%v, want %s and failover=%v", m, failover, want, tc.wantFailover)
			}
		})
	}
}

// A version is 1.2.0 or later only if it reads as one: anything else,
// empty included, counts as older.
func TestEngineAtLeast(t *testing.T) {
	for v, want := range map[string]bool{
		"1.2.0": true, "1.2.0-rc1": true, "1.2.3": true, "1.10.0": true, "2.0.0": true,
		"1.1.3": false, "1.1.99-rc1": false, "0.9.0": false, "": false, "1.2": false,
		"1.2.0.0": false, "v1.2.0": false, "1.+2.0": false, " 1.2.0": false, "1..0": false,
	} {
		if got := engineAtLeast(v, 1, 2); got != want {
			t.Errorf("engineAtLeast(%q, 1, 2) = %v, want %v", v, got, want)
		}
	}
}

// A master that stopped cleanly goes on with its stream from where it was:
// it lost nothing, whatever a replica read a moment later says.
func TestReconcileRoles_CleanlyRestartedMasterIsLeftAlone(t *testing.T) {
	h := newRolesHarness(t, "c1-0",
		testPod{name: "c1-0", ip: "10.0.0.9", label: "master", ready: true, agent: masterRun(1000, "stream-a", "run-2")},
		testPod{name: "c1-1", ip: "10.0.0.2", label: "replica", ready: true, agent: replicaOf("10.0.0.1", 1200)},
	)
	h.cluster.Status.Pods = []kividbv1alpha1.KividbPodStatus{
		{Name: "c1-0", Role: kividbv1alpha1.RoleMaster, Ready: true, Synced: true, ReplicationOffset: 1000, ReplID: "stream-a", RunID: "run-1"},
		{Name: "c1-1", Role: kividbv1alpha1.RoleReplica, Ready: true, Synced: true, ReplicationOffset: 1000},
	}
	if m, failover := h.reconcile(); m != "c1-0" || failover {
		t.Fatalf("master=%q failover=%v, want c1-0 and no failover", m, failover)
	}
}

// A master whose engine has not restarted is never failed away from on
// its offsets: a reading below the last one is a reading, not a loss.
func TestReconcileRoles_LowerOffsetWithoutARestartIsLeftAlone(t *testing.T) {
	h := newRolesHarness(t, "c1-0",
		testPod{name: "c1-0", ip: "10.0.0.1", label: "master", ready: true, agent: masterRun(28521873, "stream-b", "run-1")},
		testPod{name: "c1-1", ip: "10.0.0.2", label: "replica", ready: true, agent: linkDown(replicaOf("10.0.0.1", 28525844))},
	)
	h.cluster.Status.Pods = []kividbv1alpha1.KividbPodStatus{
		{Name: "c1-0", Role: kividbv1alpha1.RoleMaster, Ready: true, Synced: true, ReplicationOffset: 28521980, ReplID: "stream-b", RunID: "run-1"},
		{Name: "c1-1", Role: kividbv1alpha1.RoleReplica, Ready: true, Synced: true, ReplicationOffset: 28521000},
	}
	if m, failover := h.reconcile(); m != "c1-0" || failover {
		t.Fatalf("master=%q failover=%v, want c1-0 and no failover", m, failover)
	}
	for _, call := range h.agents.calls {
		if strings.HasPrefix(call, "promote") || strings.Contains(call, "PAUSE") {
			t.Fatalf("unexpected agent call: %s", call)
		}
	}
}

// A3 on AKS: a planned switchover from c1-0 to c1-1 under load, then
// reconciles whose readings of the new master are moments stale (28521980
// recorded, 28521873 read next) while c1-2, now following c1-1, reports
// more. c1-1 never restarted: it stays the master, and nothing is
// promoted or repointed at another master.
func TestReconcileRoles_NoFailoverOnStaleReadingsAfterASwitchover(t *testing.T) {
	h := newRolesHarness(t, "c1-0",
		testPod{name: "c1-0", ip: "10.0.0.1", label: "master", ready: true, uid: "u0", agent: masterRun(28518513, "stream-a", "run-0")},
		testPod{name: "c1-1", ip: "10.0.0.2", label: "replica", ready: true, uid: "u1", agent: replicaRun("10.0.0.1", 28518513, "run-1")},
		testPod{name: "c1-2", ip: "10.0.0.3", label: "replica", ready: true, uid: "u2", agent: replicaRun("10.0.0.1", 28518513, "run-2")},
	)
	h.pass() // statuses as the switchover pass reads them

	var list corev1.PodList
	if err := h.r.List(context.Background(), &list, client.InNamespace(h.cluster.Namespace)); err != nil {
		t.Fatal(err)
	}
	statuses := h.cluster.Status.Pods
	if to, err := h.r.switchover(context.Background(), h.cluster, list.Items, statuses, "c1-0", switchoverRequest{reason: "step-down"}); err != nil || to == "" {
		t.Fatalf("switchover: %q, %v", to, err)
	}
	h.cluster.Status.Pods = statuses // written as read before the switchover (masterPod stays c1-0)
	for _, s := range statuses {
		if s.ReplID != "" {
			t.Errorf("%s keeps the stream %q recorded before the role moved", s.Name, s.ReplID)
		}
	}
	newMaster := h.agents.status["10.0.0.2"]
	newMaster.MasterReplID = "stream-b" // the promoted pod goes on with the stream under a new ID

	// The passes that follow, with the new master's offset read as it was
	// a moment ago and c1-2 ahead of that reading.
	for i, o := range []int64{28521980, 28521873, 28521900} {
		newMaster.ReplicationOffset = o
		h.agents.status["10.0.0.3"].ReplicationOffset = 28525844
		h.agents.calls = nil
		if m, failover := h.pass(); m != "c1-1" || failover {
			t.Fatalf("pass %d (master offset %d): master=%q failover=%v, want c1-1 and no failover", i, o, m, failover)
		}
		for _, call := range h.agents.calls {
			if strings.HasPrefix(call, "promote") || strings.HasSuffix(call, "-> 10.0.0.3") {
				t.Fatalf("pass %d: %s", i, call)
			}
		}
	}
	h.assertLabels(map[string]string{"c1-0": "replica", "c1-1": "master", "c1-2": "replica"})
}

// From an agent that reports no run_id, the restart evidence is the pod's:
// a recreated pod (a new UID) behind its replicas is failed away from; the
// same pod, never restarted, is not.
func TestReconcileRoles_RestartEvidenceWithoutRunID(t *testing.T) {
	for _, tc := range []struct {
		name     string
		uid      types.UID
		restarts int32
		failover bool
	}{
		{"recreated", "u-new", 0, true},
		{"container restarted", "u-old", 1, true},
		{"never restarted", "u-old", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRolesHarness(t, "c1-0",
				testPod{name: "c1-0", ip: "10.0.0.9", label: "master", ready: true, uid: tc.uid, restarts: tc.restarts, agent: master(700)},
				testPod{name: "c1-1", ip: "10.0.0.2", label: "replica", ready: true, agent: replicaOf("10.0.0.1", 1000)},
			)
			h.cluster.Status.Pods = []kividbv1alpha1.KividbPodStatus{
				{Name: "c1-0", Role: kividbv1alpha1.RoleMaster, Ready: true, Synced: true, ReplicationOffset: 1000, Incarnation: "u-old/0"},
				{Name: "c1-1", Role: kividbv1alpha1.RoleReplica, Ready: true, Synced: true, ReplicationOffset: 1000},
			}
			if _, failover := h.reconcile(); failover != tc.failover {
				t.Fatalf("failover=%v, want %v", failover, tc.failover)
			}
		})
	}
}

// A replica that already follows the restarted master at its address,
// link up, has been resynced from it: there is nothing left on it to save.
func TestReconcileRoles_RestartedMasterWhoseReplicasResyncedIsLeftAlone(t *testing.T) {
	h := newRolesHarness(t, "c1-0",
		testPod{name: "c1-0", ip: "10.0.0.1", label: "master", ready: true, agent: masterRun(50, "stream-b", "run-2")},
		testPod{name: "c1-1", ip: "10.0.0.2", label: "replica", ready: true, agent: replicaRun("10.0.0.1", 60, "run-r")},
	)
	h.cluster.Status.Pods = []kividbv1alpha1.KividbPodStatus{
		{Name: "c1-0", Role: kividbv1alpha1.RoleMaster, Ready: true, Synced: true, ReplicationOffset: 1000, ReplID: "stream-a", RunID: "run-1"},
		{Name: "c1-1", Role: kividbv1alpha1.RoleReplica, Ready: true, Synced: true, ReplicationOffset: 1000},
	}
	if m, failover := h.reconcile(); m != "c1-0" || failover {
		t.Fatalf("master=%q failover=%v, want c1-0 and no failover", m, failover)
	}
}

// The status records a master's stream only from the master itself, and
// none for a replica.
func TestReconcileRoles_RecordsStreamOnlyForTheMaster(t *testing.T) {
	h := newRolesHarness(t, "c1-0",
		testPod{name: "c1-0", ip: "10.0.0.1", label: "master", ready: true, uid: "u0", agent: masterRun(100, "stream-a", "run-0")},
		testPod{name: "c1-1", ip: "10.0.0.2", label: "replica", ready: true, uid: "u1", restarts: 2, agent: replicaRun("10.0.0.1", 100, "run-1")},
	)
	h.pass()
	want := map[string]kividbv1alpha1.KividbPodStatus{
		"c1-0": {ReplID: "stream-a", RunID: "run-0", Incarnation: "u0/0"},
		"c1-1": {ReplID: "", RunID: "run-1", Incarnation: "u1/2"},
	}
	for _, s := range h.cluster.Status.Pods {
		w := want[s.Name]
		if s.ReplID != w.ReplID || s.RunID != w.RunID || s.Incarnation != w.Incarnation {
			t.Errorf("%s: replID=%q runID=%q incarnation=%q, want %q %q %q", s.Name, s.ReplID, s.RunID, s.Incarnation, w.ReplID, w.RunID, w.Incarnation)
		}
	}
}

func masterRun(offset int64, replID, runID string) *agentapi.StatusResponse {
	st := master(offset)
	st.MasterReplID, st.RunID = replID, runID
	return st
}

// linkDown marks a replica's link to its master as down (reconnecting).
func linkDown(st *agentapi.StatusResponse) *agentapi.StatusResponse {
	down := false
	st.MasterLinkUp = &down
	return st
}

// keyed sets the number of keys an agent reports.
func keyed(st *agentapi.StatusResponse, keys int64) *agentapi.StatusResponse {
	st.KeyCount, st.KeyCountKnown = keys, true
	return st
}

func replicaRun(host string, offset int64, runID string) *agentapi.StatusResponse {
	st := replicaOf(host, offset)
	st.RunID = runID
	return st
}

// pass runs one reconcileRoles pass and persists its statuses and master
// the way updateStatus would.
func (h *rolesHarness) pass() (masterPod string, failover bool) {
	h.t.Helper()
	var list corev1.PodList
	if err := h.r.List(context.Background(), &list, client.InNamespace(h.cluster.Namespace)); err != nil {
		h.t.Fatal(err)
	}
	statuses, masterPod, failover, err := h.r.reconcileRoles(context.Background(), h.cluster, list.Items)
	if err != nil {
		h.t.Fatalf("reconcileRoles: %v", err)
	}
	h.cluster.Status.Pods, h.cluster.Status.MasterPod = statuses, masterPod
	return masterPod, failover
}

// withClusterInClient puts the cluster into the fake client, with its
// status subresource, so updateStatus can write it.
func (h *rolesHarness) withClusterInClient() {
	h.t.Helper()
	pods := h.list()
	if err := kividbv1alpha1.AddToScheme(h.r.Scheme); err != nil {
		h.t.Fatal(err)
	}
	b := fake.NewClientBuilder().WithScheme(h.r.Scheme).WithObjects(h.cluster).
		WithStatusSubresource(&kividbv1alpha1.KividbCluster{})
	for i := range pods {
		pods[i].ResourceVersion = ""
		b = b.WithObjects(&pods[i])
	}
	h.r.Client = b.Build()
}

// assertCallOrder checks that the agent calls include want, in this order.
func (h *rolesHarness) assertCallOrder(want ...string) {
	h.t.Helper()
	i := 0
	for _, call := range h.agents.calls {
		if i < len(want) && call == want[i] {
			i++
		}
	}
	if i < len(want) {
		h.t.Errorf("agent calls %v: missing %q (in this order: %v)", h.agents.calls, want[i], want)
	}
}

// A master whose offset has only grown is left alone, even when a replica
// read a moment later reports more (it applied writes in between).
func TestReconcileRoles_MasterAheadOfItsLastOffsetIsLeftAlone(t *testing.T) {
	h := newRolesHarness(t, "c1-0",
		testPod{name: "c1-0", ip: "10.0.0.1", label: "master", ready: true, agent: master(1200)},
		testPod{name: "c1-1", ip: "10.0.0.2", label: "replica", ready: true, agent: replicaOf("10.0.0.1", 1300)},
	)
	h.cluster.Status.Pods = []kividbv1alpha1.KividbPodStatus{
		{Name: "c1-0", Role: kividbv1alpha1.RoleMaster, Ready: true, Synced: true, ReplicationOffset: 1000},
		{Name: "c1-1", Role: kividbv1alpha1.RoleReplica, Ready: true, Synced: true, ReplicationOffset: 1000},
	}
	if m, failover := h.reconcile(); m != "c1-0" || failover {
		t.Fatalf("master=%q failover=%v, want c1-0 and no failover", m, failover)
	}
	if len(h.agents.calls) != 0 {
		t.Fatalf("unexpected agent calls: %v", h.agents.calls)
	}
}

// While the master cannot be asked, its last offset and key count stay in
// the status: they are what recognises it coming back behind.
func TestReconcileRoles_UnreachablePodKeepsItsLastOffset(t *testing.T) {
	h := newRolesHarness(t, "c1-0",
		testPod{name: "c1-0", ip: "10.0.0.1", label: "master", unreadyFor: 5 * time.Second},
		testPod{name: "c1-1", ip: "10.0.0.2", label: "replica", ready: true, agent: replicaOf("10.0.0.1", 1000)},
	)
	h.cluster.Status.Pods = []kividbv1alpha1.KividbPodStatus{
		{Name: "c1-0", Role: kividbv1alpha1.RoleMaster, Ready: true, Synced: true, ReplicationOffset: 1000, Keys: 42},
	}
	statuses, _, _, err := h.r.reconcileRoles(context.Background(), h.cluster, h.list())
	if err != nil {
		t.Fatal(err)
	}
	if s := statuses[0]; s.Name != "c1-0" || s.ReplicationOffset != 1000 || s.Keys != 42 {
		t.Fatalf("status of the unreachable master: %+v", s)
	}
}

// failoverCase is a replicated cluster c1 whose master c1-0 has been
// NotReady for a minute (past its 20s threshold) while its replica c1-1 is
// healthy: a pass that reaches the roles promotes c1-1.
type failoverCase struct {
	c      *kividbv1alpha1.KividbCluster
	r      *KividbClusterReconciler
	cl     client.Client
	agents *fakeAgents
}

// newFailoverCase builds it with objs besides the cluster and its pods;
// edit changes the cluster first, refuse makes the API server refuse a
// Create or Update.
func newFailoverCase(t *testing.T, edit func(*kividbv1alpha1.KividbCluster), refuse func(client.Object) bool, objs ...client.Object) *failoverCase {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := kividbv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	threshold := int32(20)
	c := &kividbv1alpha1.KividbCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "default", UID: "u1"},
		Spec:       kividbv1alpha1.KividbClusterSpec{Replicas: 1, Storage: kividbv1alpha1.StorageSpec{Size: "1Gi"}},
	}
	c.Spec.Failover.UnhealthyThresholdSeconds = &threshold
	if edit != nil {
		edit(c)
	}
	pod := func(name, ip, role string, ready bool) *corev1.Pod {
		labels := selectorLabels(c)
		labels[kividbv1alpha1.RoleLabel] = role
		cond := corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue}
		if !ready {
			cond.Status = corev1.ConditionFalse
			cond.LastTransitionTime = metav1.NewTime(time.Now().Add(-time.Minute))
		}
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: c.Namespace, Labels: labels},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: kividbContainerName,
				Ports: []corev1.ContainerPort{{Name: KividbPortName, ContainerPort: 6380}}}}},
			Status: corev1.PodStatus{PodIP: ip, Conditions: []corev1.PodCondition{cond},
				ContainerStatuses: []corev1.ContainerStatus{{Name: kividbContainerName}}},
		}
	}
	refused := func(obj client.Object) error {
		if refuse != nil && refuse(obj) {
			return fmt.Errorf("%s is invalid", obj.GetName())
		}
		return nil
	}
	objs = append(objs, c, pod("c1-0", "10.0.0.1", "master", false), pod("c1-1", "10.0.0.2", "replica", true))
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
		WithStatusSubresource(&kividbv1alpha1.KividbCluster{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if err := refused(obj); err != nil {
					return err
				}
				return cl.Create(ctx, obj, opts...)
			},
			Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if err := refused(obj); err != nil {
					return err
				}
				return cl.Update(ctx, obj, opts...)
			},
		}).Build()
	agents := &fakeAgents{status: map[string]*agentapi.StatusResponse{
		"10.0.0.1": master(100),
		"10.0.0.2": replicaOf("10.0.0.1", 100),
	}}
	r := &KividbClusterReconciler{Client: cl, Scheme: scheme, Agent: &AgentClient{http: &http.Client{Transport: agents}}}
	// The status subresource drops what WithObjects was given; set it.
	c.Status.MasterPod = "c1-0"
	c.Status.Phase = kividbv1alpha1.PhaseRunning
	c.Status.Conditions = []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue, Reason: "AllPodsReady", Message: "master=c1-0, 2 pod(s) ready", LastTransitionTime: metav1.Now()}}
	if err := r.Status().Update(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	return &failoverCase{c: c, r: r, cl: cl, agents: agents}
}

// reconcile runs one pass, which must come back for another look in
// requeue without an error: an error return would put the retry on the
// rate limiter's backoff, which grows to minutes.
func (f *failoverCase) reconcile(t *testing.T, requeue time.Duration) *kividbv1alpha1.KividbCluster {
	t.Helper()
	ctx := context.Background()
	res, err := f.r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.c)})
	if err != nil || res.RequeueAfter != requeue {
		t.Errorf("Reconcile = %+v, %v; want a requeue in %s and no error", res, err, requeue)
	}
	var got kividbv1alpha1.KividbCluster
	if err := f.cl.Get(ctx, client.ObjectKeyFromObject(f.c), &got); err != nil {
		t.Fatal(err)
	}
	return &got
}

// ready is the cluster's Ready condition as stored.
func (f *failoverCase) ready(t *testing.T) *metav1.Condition {
	t.Helper()
	var got kividbv1alpha1.KividbCluster
	if err := f.cl.Get(context.Background(), client.ObjectKeyFromObject(f.c), &got); err != nil {
		t.Fatal(err)
	}
	return meta.FindStatusCondition(got.Status.Conditions, "Ready")
}

// wantFailedOver checks c1-1 was promoted and Ready is False for reason,
// with a message that says msg.
func (f *failoverCase) wantFailedOver(t *testing.T, got *kividbv1alpha1.KividbCluster, reason, msg string) {
	t.Helper()
	if got.Status.MasterPod != "c1-1" {
		t.Errorf("status.masterPod = %q, want c1-1 promoted (agent calls %v)", got.Status.MasterPod, f.agents.calls)
	}
	ready := meta.FindStatusCondition(got.Status.Conditions, "Ready")
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != reason || !strings.Contains(ready.Message, msg) {
		t.Errorf("Ready condition %+v, want False, %s, %q", ready, reason, msg)
	}
}

// An object the API server refuses to apply -- a backup CronJob, Services
// -- must not stop failover, nor leave the cluster Ready. The steps after
// the refused one are skipped.
func TestReconcile_RefusedApplyStepStillFailsOver(t *testing.T) {
	snap := &kividbv1alpha1.KividbSnapshotConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "snap", Namespace: "default"},
		Spec: kividbv1alpha1.KividbSnapshotConfigSpec{Schedule: "0 2 * * *",
			S3: kividbv1alpha1.S3StorageSpec{Endpoint: "s3.example", Bucket: "b", CredentialsSecretRef: kividbv1alpha1.S3CredentialsSecretRef{Name: "s3"}}},
	}
	for _, tc := range []struct {
		name    string
		refuse  func(client.Object) bool
		wantMsg string
		// noSTS: the StatefulSet, applied after the refused step, is not.
		noSTS bool
	}{
		{"backup CronJob", func(obj client.Object) bool {
			_, ok := obj.(*batchv1.CronJob)
			return ok
		}, "reconciling backup cronjob", false},
		{"Services", func(obj client.Object) bool {
			_, ok := obj.(*corev1.Service)
			return ok
		}, "reconciling services", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFailoverCase(t, func(c *kividbv1alpha1.KividbCluster) {
				c.Spec.SnapshotConfigRef = &corev1.LocalObjectReference{Name: "snap"}
			}, tc.refuse, snap.DeepCopy())
			got := f.reconcile(t, 5*time.Second)
			f.wantFailedOver(t, got, "ReconcileError", tc.wantMsg)
			if tc.noSTS {
				var sts appsv1.StatefulSet
				if err := f.cl.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: statefulSetName(f.c)}, &sts); !apierrors.IsNotFound(err) {
					t.Errorf("StatefulSet applied after the Services were refused (get: %v)", err)
				}
			}
		})
	}
}

// wantNothingWritten checks that none of the cluster's objects was
// written: rendered without a reference they would open the default user,
// drop the directives or remove the backup CronJob.
func (f *failoverCase) wantNothingWritten(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	for _, o := range []struct {
		name string
		obj  client.Object
	}{
		{secretName(f.c), &corev1.Secret{}},
		{configSecretName(f.c), &corev1.Secret{}},
		{headlessServiceName(f.c), &corev1.Service{}},
		{statefulSetName(f.c), &appsv1.StatefulSet{}},
		{backupCronJobName(f.c), &batchv1.CronJob{}},
	} {
		if err := f.cl.Get(ctx, client.ObjectKey{Namespace: f.c.Namespace, Name: o.name}, o.obj); !apierrors.IsNotFound(err) {
			t.Errorf("%T %s was written (get: %v)", o.obj, o.name, err)
		}
	}
}

// A referenced object that cannot be read -- deleted, or not made yet by
// whatever makes it -- holds back every write, but not failover, and the
// error shows on the Ready condition.
func TestReconcile_UnreadableReferenceStillFailsOver(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*kividbv1alpha1.KividbCluster)
		objs []client.Object
		want string
	}{
		{"KividbConfig", func(c *kividbv1alpha1.KividbCluster) {
			c.Spec.ConfigRef = &corev1.LocalObjectReference{Name: "conf"}
		}, nil, `resolving configRef: KividbConfig "conf"`},
		{"KividbAclConfig", func(c *kividbv1alpha1.KividbCluster) {
			c.Spec.AclConfigRef = &corev1.LocalObjectReference{Name: "acl"}
		}, nil, `resolving aclConfigRef: KividbAclConfig "acl"`},
		{"KividbSnapshotConfig", func(c *kividbv1alpha1.KividbCluster) {
			c.Spec.SnapshotConfigRef = &corev1.LocalObjectReference{Name: "snap"}
		}, nil, `resolving snapshotConfigRef: KividbSnapshotConfig "snap"`},
		{"password Secret", func(c *kividbv1alpha1.KividbCluster) {
			c.Spec.AclConfigRef = &corev1.LocalObjectReference{Name: "acl"}
		}, []client.Object{&kividbv1alpha1.KividbAclConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "acl", Namespace: "default"},
			Spec:       kividbv1alpha1.KividbAclConfigSpec{RequirePassSecretRef: &kividbv1alpha1.SecretKeyRef{Name: "pw", Key: "password"}},
		}}, "resolving referenced secrets: secret pw"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFailoverCase(t, tc.edit, nil, tc.objs...)
			got := f.reconcile(t, 5*time.Second)
			f.wantFailedOver(t, got, "ReconcileError", tc.want)
			f.wantNothingWritten(t)
		})
	}
}

// A port that clashes, or a backup schedule CronJobs do not accept, is
// refused before anything is written, and failover goes on.
func TestReconcile_InvalidReferenceStillFailsOver(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*kividbv1alpha1.KividbCluster)
		obj  client.Object
		want string
	}{
		{"tls.port is the cluster port", func(c *kividbv1alpha1.KividbCluster) {
			c.Spec.ConfigRef = &corev1.LocalObjectReference{Name: "conf"}
		}, &kividbv1alpha1.KividbConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "conf", Namespace: "default"},
			Spec:       kividbv1alpha1.KividbConfigSpec{TLS: &kividbv1alpha1.KividbTLSSpec{Enabled: true, Port: 6380}},
		}, "tls.port 6380 is also"},
		{"schedule with four fields", func(c *kividbv1alpha1.KividbCluster) {
			c.Spec.SnapshotConfigRef = &corev1.LocalObjectReference{Name: "snap"}
		}, &kividbv1alpha1.KividbSnapshotConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "snap", Namespace: "default"},
			Spec:       kividbv1alpha1.KividbSnapshotConfigSpec{Schedule: "0 2 * *"},
		}, `KividbSnapshotConfig snap: spec.schedule "0 2 * *"`},
		{"schedule with a time zone", func(c *kividbv1alpha1.KividbCluster) {
			c.Spec.SnapshotConfigRef = &corev1.LocalObjectReference{Name: "snap"}
		}, &kividbv1alpha1.KividbSnapshotConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "snap", Namespace: "default"},
			Spec:       kividbv1alpha1.KividbSnapshotConfigSpec{Schedule: "CRON_TZ=UTC 0 2 * * *"},
		}, `KividbSnapshotConfig snap: spec.schedule "CRON_TZ=UTC 0 2 * * *"`},
		{"schedule the cron library panics on", func(c *kividbv1alpha1.KividbCluster) {
			c.Spec.SnapshotConfigRef = &corev1.LocalObjectReference{Name: "snap"}
		}, &kividbv1alpha1.KividbSnapshotConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "snap", Namespace: "default"},
			Spec:       kividbv1alpha1.KividbSnapshotConfigSpec{Schedule: "TZ=UTC"},
		}, `KividbSnapshotConfig snap: spec.schedule "TZ=UTC"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFailoverCase(t, tc.edit, nil, tc.obj)
			got := f.reconcile(t, reconcileInterval)
			f.wantFailedOver(t, got, "InvalidSpec", tc.want)
			f.wantNothingWritten(t)
		})
	}
}

// Held back with spec.port changed, the pods still listen on the old port:
// the roles, which point replicas at spec.port, are left alone.
func TestReconcile_HeldBackPortChangeLeavesRolesAlone(t *testing.T) {
	f := newFailoverCase(t, func(c *kividbv1alpha1.KividbCluster) {
		c.Spec.Port = 6390
		c.Spec.ConfigRef = &corev1.LocalObjectReference{Name: "conf"}
	}, nil)
	got := f.reconcile(t, 5*time.Second)
	if len(f.agents.calls) != 0 || got.Status.MasterPod != "c1-0" {
		t.Errorf("roles changed (master %q, agent calls %v), want none: the pods are not on port 6390 yet", got.Status.MasterPod, f.agents.calls)
	}
	ready := meta.FindStatusCondition(got.Status.Conditions, "Ready")
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "ReconcileError" || !strings.Contains(ready.Message, `KividbConfig "conf"`) {
		t.Errorf("Ready condition %+v", ready)
	}
}

// A change to an object a cluster references reconciles that cluster, and
// only the clusters that reference it.
func TestReferencedObjectsEnqueueTheirClusters(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := kividbv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	ref := func(name string) *corev1.LocalObjectReference { return &corev1.LocalObjectReference{Name: name} }
	cluster := func(name, ns string, edit func(*kividbv1alpha1.KividbCluster)) *kividbv1alpha1.KividbCluster {
		c := &kividbv1alpha1.KividbCluster{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}}
		edit(c)
		return c
	}
	acl := &kividbv1alpha1.KividbAclConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "acl", Namespace: "a"},
		Spec: kividbv1alpha1.KividbAclConfigSpec{
			RequirePassSecretRef: &kividbv1alpha1.SecretKeyRef{Name: "pw", Key: "password"},
			Users:                []kividbv1alpha1.KividbUser{{Name: "app", PasswordSecretRef: &kividbv1alpha1.SecretKeyRef{Name: "app-pw", Key: "password"}}},
		},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		acl,
		cluster("one", "a", func(c *kividbv1alpha1.KividbCluster) {
			c.Spec.ConfigRef, c.Spec.AclConfigRef, c.Spec.SnapshotConfigRef = ref("conf"), ref("acl"), ref("snap")
		}),
		cluster("two", "a", func(c *kividbv1alpha1.KividbCluster) {
			c.Spec.ConfigRef = ref("conf")
			c.Spec.Cluster = &kividbv1alpha1.ClusterModeSpec{BusSecretRef: &kividbv1alpha1.SecretKeyRef{Name: "bus", Key: "secret"}}
		}),
		cluster("three", "a", func(c *kividbv1alpha1.KividbCluster) { c.Spec.ConfigRef = ref("other") }),
		cluster("four", "b", func(c *kividbv1alpha1.KividbCluster) { c.Spec.ConfigRef = ref("conf") }),
	).Build()
	r := &KividbClusterReconciler{Client: cl, Scheme: scheme}
	in := func(name string) metav1.ObjectMeta { return metav1.ObjectMeta{Name: name, Namespace: "a"} }
	for _, tc := range []struct {
		obj  client.Object
		want string
	}{
		{&kividbv1alpha1.KividbConfig{ObjectMeta: in("conf")}, "a/one a/two"},
		{&kividbv1alpha1.KividbConfig{ObjectMeta: in("snap")}, ""},
		{&kividbv1alpha1.KividbAclConfig{ObjectMeta: in("acl")}, "a/one"},
		{&kividbv1alpha1.KividbSnapshotConfig{ObjectMeta: in("snap")}, "a/one"},
		{&corev1.Secret{ObjectMeta: in("pw")}, "a/one"},
		{&corev1.Secret{ObjectMeta: in("app-pw")}, "a/one"},
		{&corev1.Secret{ObjectMeta: in("bus")}, "a/two"},
		{&corev1.Secret{ObjectMeta: in("unrelated")}, ""},
	} {
		var got []string
		for _, req := range r.mapReferenceToClusters(context.Background(), tc.obj) {
			got = append(got, req.String())
		}
		sort.Strings(got)
		if strings.Join(got, " ") != tc.want {
			t.Errorf("%T %s enqueues %v, want %q", tc.obj, tc.obj.GetName(), got, tc.want)
		}
	}
}

// setPodPort makes the pods' kividb container declare port, as pods made
// with spec.port = port do.
func (f *failoverCase) setPodPort(t *testing.T, port int32) {
	t.Helper()
	ctx := context.Background()
	for _, name := range []string{"c1-0", "c1-1"} {
		var p corev1.Pod
		if err := f.cl.Get(ctx, client.ObjectKey{Namespace: "default", Name: name}, &p); err != nil {
			t.Fatal(err)
		}
		p.Spec.Containers[0].Ports[0].ContainerPort = port
		if err := f.cl.Update(ctx, &p); err != nil {
			t.Fatal(err)
		}
	}
}

// A port the cluster's own spec gives that clashes with another listener
// of the pod -- monitoring turned on for a cluster on the exporter's port
// -- is refused before anything is written, and failover goes on.
func TestReconcile_PortClashStillFailsOver(t *testing.T) {
	f := newFailoverCase(t, func(c *kividbv1alpha1.KividbCluster) {
		c.Spec.Port = ExporterPort
		c.Spec.Monitoring.Enabled = true
	}, nil)
	f.setPodPort(t, ExporterPort)
	got := f.reconcile(t, reconcileInterval)
	f.wantFailedOver(t, got, "InvalidSpec", "spec.port 9121 is also the metrics exporter's port")
	f.wantNothingWritten(t)
}
