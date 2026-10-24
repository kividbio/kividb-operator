package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	"github.com/kividbio/kividb-operator/internal/agentapi"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// fakeEngine is a toy kividb cluster behind the agents' cluster endpoints:
// enough to check what the operator asks of it, not how kividb gossips.
type fakeEngine struct {
	mu    sync.Mutex
	nodes map[string]*fakeNode // by node ID
	byIP  map[string]string    // pod IP -> node ID
	calls []string

	tasks      map[string]string // migration task ID -> state
	failImport bool              // the next import fails
	moved      int               // slots moved by imports
}

type fakeNode struct {
	id, ip, master, hostname string
	seen                     map[string]string // node ID -> the IP this node has for it
	slots                    [][2]int
	offset                   int64
	linkDown                 bool // a replica whose link to its master is down
}

func newFakeEngine() *fakeEngine {
	return &fakeEngine{nodes: map[string]*fakeNode{}, byIP: map[string]string{}, tasks: map[string]string{}}
}

func nodeID(name string) string {
	return fmt.Sprintf("%040x", []byte(name))[:40]
}

func (e *fakeEngine) add(id, ip string) *fakeNode {
	n := &fakeNode{id: id, ip: ip, seen: map[string]string{}}
	e.nodes[id] = n
	e.byIP[ip] = id
	return n
}

func (e *fakeEngine) status(n *fakeNode) agentapi.ClusterStatusResponse {
	ids := []string{n.id}
	for id := range n.seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var lines []string
	covered := 0
	for _, id := range ids {
		o, ok := e.nodes[id]
		if !ok {
			// Gone (its pod was removed): still known, as failed.
			lines = append(lines, fmt.Sprintf("%s %s:6380@16380 master,fail - 0 0 1 disconnected", id, n.seen[id]))
			continue
		}
		flags := "master"
		if o.master != "" {
			flags = "slave"
		}
		ip := n.seen[id]
		if id == n.id {
			flags = "myself," + flags
			ip = n.ip
		}
		master := "-"
		if o.master != "" {
			master = o.master
		}
		addr := ip + ":6380@16380"
		if o.hostname != "" {
			addr += "," + o.hostname
		}
		var slots []string
		for _, s := range o.slots {
			slots = append(slots, fmt.Sprintf("%d-%d", s[0], s[1]))
			covered += s[1] - s[0] + 1
		}
		lines = append(lines, strings.TrimSpace(fmt.Sprintf("%s %s %s %s 0 0 1 connected %s", id, addr, flags, master, strings.Join(slots, " "))))
	}
	state := "fail"
	if covered == agentapi.TotalSlots {
		state = "ok"
	}
	return agentapi.ClusterStatusResponse{
		Enabled: true, MyID: n.id, State: state, SlotsAssigned: covered, KnownNodes: len(ids),
		Hostname: n.hostname, Nodes: agentapi.ParseClusterNodes(strings.Join(lines, "\n")),
		ReplicationOffset: n.offset, MasterLinkUp: n.master != "" && !n.linkDown,
	}
}

func (e *fakeEngine) RoundTrip(req *http.Request) (*http.Response, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	ip := strings.Split(req.URL.Host, ":")[0]
	n := e.nodes[e.byIP[ip]]
	if n == nil {
		return nil, fmt.Errorf("no node at %s", ip)
	}
	if !agentapi.TokenMatches(req, "tok") {
		return reply(http.StatusUnauthorized, nil), nil
	}
	var body struct {
		IP       string   `json:"ip"`
		NodeID   string   `json:"nodeID"`
		Hostname string   `json:"hostname"`
		Ranges   [][2]int `json:"ranges"`
	}
	if req.Body != nil {
		_ = json.NewDecoder(req.Body).Decode(&body)
	}
	if req.URL.Path != "/cluster/status" && req.URL.Path != "/cluster/migration/status" {
		e.calls = append(e.calls, fmt.Sprintf("%s %s %s%s", req.URL.Path, n.id[:6], body.IP, body.NodeID))
	}
	switch req.URL.Path {
	case "/cluster/status":
		st := e.status(n)
		return reply(http.StatusOK, st), nil
	case "/cluster/migration/import":
		return e.importSlots(n, body.Ranges)
	case "/cluster/migration/status":
		id := req.URL.Query().Get("id")
		state, ok := e.tasks[id]
		if !ok {
			return reply(http.StatusNotFound, agentapi.ErrorResponse{Error: "no such task"}), nil
		}
		return reply(http.StatusOK, agentapi.ClusterMigrationTask{ID: id, State: state, LastError: map[bool]string{true: "source went away"}[state == agentapi.MigrationFailed]}), nil
	case "/cluster/meet":
		o := e.nodes[e.byIP[body.IP]]
		if o == nil {
			return reply(http.StatusInternalServerError, agentapi.ErrorResponse{Error: "unreachable"}), nil
		}
		// The handshake, and a round of gossip both ways.
		n.seen[o.id], o.seen[n.id] = o.ip, n.ip
		for id, ipSeen := range o.seen {
			if id != n.id {
				if _, ok := n.seen[id]; !ok {
					n.seen[id] = ipSeen
				}
			}
		}
		for id, ipSeen := range n.seen {
			if id != o.id {
				if _, ok := o.seen[id]; !ok {
					o.seen[id] = ipSeen
				}
			}
		}
	case "/cluster/addslots":
		n.slots = append(n.slots, body.Ranges...)
	case "/cluster/replicate":
		n.master = body.NodeID
	case "/cluster/forget":
		delete(n.seen, body.NodeID)
	case "/cluster/announce":
		n.hostname = body.Hostname
	case "/cluster/failover":
		old := e.nodes[n.master]
		if old == nil {
			return reply(http.StatusInternalServerError, agentapi.ErrorResponse{Error: "You should send CLUSTER FAILOVER to a replica"}), nil
		}
		n.slots, old.slots = old.slots, nil
		for _, o := range e.nodes {
			if o.master == old.id {
				o.master = n.id
			}
		}
		n.master, old.master = "", n.id
	}
	return reply(http.StatusOK, agentapi.OKResponse{OK: true}), nil
}

// importSlots is CLUSTER MIGRATION IMPORT on n, done at once: every
// slot must belong to one other master.
func (e *fakeEngine) importSlots(n *fakeNode, ranges [][2]int) (*http.Response, error) {
	if n.master != "" {
		return reply(http.StatusInternalServerError, agentapi.ErrorResponse{Error: "slots can only be imported by a master"}), nil
	}
	var source *fakeNode
	for _, rg := range ranges {
		for s := rg[0]; s <= rg[1]; s++ {
			o := e.owner(s)
			if o == nil || o == n || (source != nil && o != source) {
				return reply(http.StatusInternalServerError, agentapi.ErrorResponse{Error: fmt.Sprintf("slot %d: not one other owner", s)}), nil
			}
			source = o
		}
	}
	id := fmt.Sprintf("%040x", len(e.tasks)+1)
	if e.failImport {
		e.failImport = false
		e.tasks[id] = agentapi.MigrationFailed
		return reply(http.StatusOK, agentapi.ClusterImportResponse{TaskID: id}), nil
	}
	take := map[int]bool{}
	for _, rg := range ranges {
		for s := rg[0]; s <= rg[1]; s++ {
			take[s] = true
		}
	}
	e.moved += len(take)
	source.slots = subtractSlots(source.slots, take)
	n.slots = addSlots(n.slots, take)
	e.tasks[id] = agentapi.MigrationCompleted
	return reply(http.StatusOK, agentapi.ClusterImportResponse{TaskID: id}), nil
}

func (e *fakeEngine) owner(slot int) *fakeNode {
	for _, o := range e.nodes {
		for _, rg := range o.slots {
			if slot >= rg[0] && slot <= rg[1] {
				return o
			}
		}
	}
	return nil
}

func rangesOf(set map[int]bool) [][2]int {
	var slots []int
	for s := range set {
		slots = append(slots, s)
	}
	sort.Ints(slots)
	var out [][2]int
	for i := 0; i < len(slots); {
		j := i
		for j+1 < len(slots) && slots[j+1] == slots[j]+1 {
			j++
		}
		out = append(out, [2]int{slots[i], slots[j]})
		i = j + 1
	}
	return out
}

func slotSet(ranges [][2]int) map[int]bool {
	set := map[int]bool{}
	for _, rg := range ranges {
		for s := rg[0]; s <= rg[1]; s++ {
			set[s] = true
		}
	}
	return set
}

func subtractSlots(ranges [][2]int, take map[int]bool) [][2]int {
	set := slotSet(ranges)
	for s := range take {
		delete(set, s)
	}
	return rangesOf(set)
}

func addSlots(ranges [][2]int, take map[int]bool) [][2]int {
	set := slotSet(ranges)
	for s := range take {
		set[s] = true
	}
	return rangesOf(set)
}

func slotCount(ranges [][2]int) int { return len(slotSet(ranges)) }

func reply(status int, v any) *http.Response {
	b, _ := json.Marshal(v)
	return &http.Response{StatusCode: status, Body: io.NopCloser(bytes.NewReader(b)), Header: http.Header{}}
}

func shardedPod(c *kividbv1alpha1.KividbCluster, shard, n int32, ip string) *corev1.Pod {
	labels := shardSelectorLabels(c, shard)
	name := shardPodName(c, shard, n)
	labels[podNameLabel] = name
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: c.Namespace, Labels: labels},
		Status: corev1.PodStatus{
			PodIP:      ip,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
}

// formingHarness: a sharded cluster, its pods, and a fake engine behind
// their agents.
type formingHarness struct {
	t       *testing.T
	c       *kividbv1alpha1.KividbCluster
	r       *KividbClusterReconciler
	engine  *fakeEngine
	exposed map[string]exposure
}

func newFormingHarness(t *testing.T, shards, replicas int32) *formingHarness {
	c := shardedCluster("s", shards, replicas)
	engine := newFakeEngine()
	var objs []client.Object
	for s := int32(0); s < shards; s++ {
		for n := int32(0); n <= replicas; n++ {
			ip := fmt.Sprintf("10.0.%d.%d", s, n+1)
			p := shardedPod(c, s, n, ip)
			objs = append(objs, p)
			engine.add(nodeID(p.Name), ip)
		}
	}
	r := shardedReconciler(t, objs...)
	r.Agent.http.Transport = engine
	return &formingHarness{t: t, c: c, r: r, engine: engine}
}

func (h *formingHarness) pods() []corev1.Pod {
	var list corev1.PodList
	if err := h.r.List(context.Background(), &list, client.InNamespace(h.c.Namespace)); err != nil {
		h.t.Fatal(err)
	}
	return list.Items
}

// pass runs one reconcile of the topology and the status.
func (h *formingHarness) pass() []*shardPod {
	ctx := context.Background()
	observed := h.r.observeShardedPods(ctx, h.c, h.pods())
	if _, err := h.r.reconcileClusterTopology(ctx, h.c, observed, h.exposed); err != nil {
		h.t.Fatal(err)
	}
	observed = h.r.observeShardedPods(ctx, h.c, h.pods())
	h.r.labelShardedRoles(ctx, observed)
	before := append([]kividbv1alpha1.ShardStatus(nil), h.c.Status.Shards...)
	h.c.Status.Phase, _, _ = shardedStatus(h.c, observed)
	h.r.noteShardFailovers(h.c, before, observed)
	return observed
}

func (h *formingHarness) node(pod string) *fakeNode { return h.engine.nodes[nodeID(pod)] }

func TestEvenSlotRanges(t *testing.T) {
	got := evenSlotRanges(3)
	want := [][2]int{{0, 5461}, {5462, 10922}, {10923, 16383}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("%v, want %v", got, want)
	}
	for n := 1; n <= 24; n++ {
		r := evenSlotRanges(n)
		if r[0][0] != 0 || r[n-1][1] != agentapi.TotalSlots-1 {
			t.Fatalf("%d shards: %v", n, r)
		}
		for i := 1; i < n; i++ {
			if r[i][0] != r[i-1][1]+1 {
				t.Fatalf("%d shards: gap at %d: %v", n, i, r)
			}
		}
	}
}

func TestClusterIsFormed(t *testing.T) {
	h := newFormingHarness(t, 3, 1)
	for i := 0; i < 4; i++ {
		h.pass()
	}
	ranges := evenSlotRanges(3)
	for s := int32(0); s < 3; s++ {
		master := h.node(shardPodName(h.c, s, 0))
		if len(master.slots) != 1 || master.slots[0] != ranges[s] {
			t.Errorf("shard %d master slots %v, want %v", s, master.slots, ranges[s])
		}
		replica := h.node(shardPodName(h.c, s, 1))
		if replica.master != master.id || len(replica.slots) != 0 {
			t.Errorf("shard %d replica follows %q, want %q", s, replica.master, master.id)
		}
		for _, n := range []*fakeNode{master, replica} {
			if !strings.HasSuffix(n.hostname, ".s-"+fmt.Sprint(s)+"-headless.default.svc") {
				t.Errorf("hostname %q", n.hostname)
			}
		}
	}
	if h.c.Status.Phase != kividbv1alpha1.PhaseRunning {
		t.Fatalf("phase %s", h.c.Status.Phase)
	}
	if h.c.Status.Shards[1].MasterPod != "s-1-0" || h.c.Status.Shards[1].Slots[0] != "5462-10922" {
		t.Fatalf("shard status: %+v", h.c.Status.Shards[1])
	}
	var pod corev1.Pod
	_ = h.r.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "s-2-1"}, &pod)
	if pod.Labels[kividbv1alpha1.RoleLabel] != "replica" {
		t.Fatalf("s-2-1 role label %q", pod.Labels[kividbv1alpha1.RoleLabel])
	}

	// Formed: another pass changes nothing.
	h.engine.calls = nil
	h.pass()
	if len(h.engine.calls) != 0 {
		t.Fatalf("a formed cluster was changed: %v", h.engine.calls)
	}
}

func TestSlotsWaitForEveryPod(t *testing.T) {
	h := newFormingHarness(t, 2, 1)
	// One pod is not ready: the cluster is not formed without it.
	var pod corev1.Pod
	_ = h.r.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "s-1-1"}, &pod)
	pod.Status.Conditions[0].Status = corev1.ConditionFalse
	if err := h.r.Status().Update(context.Background(), &pod); err != nil {
		_ = h.r.Update(context.Background(), &pod)
	}
	for i := 0; i < 3; i++ {
		h.pass()
	}
	for _, n := range h.engine.nodes {
		if len(n.slots) != 0 {
			t.Fatalf("slots assigned before every pod was up: %s %v", n.id, n.slots)
		}
	}
}

func TestEngineFailoverIsNotUndone(t *testing.T) {
	h := newFormingHarness(t, 2, 1)
	for i := 0; i < 4; i++ {
		h.pass()
	}
	// The engine fails shard 0 over: s-0-1 takes the slots, s-0-0 comes
	// back as its replica.
	old, promoted := h.node("s-0-0"), h.node("s-0-1")
	promoted.slots, old.slots = old.slots, nil
	promoted.master, old.master = "", promoted.id

	h.engine.calls = nil
	h.pass()
	for _, call := range h.engine.calls {
		if strings.HasPrefix(call, "/cluster/replicate") {
			t.Fatalf("the operator undid the failover: %v", h.engine.calls)
		}
	}
	if h.c.Status.Shards[0].MasterPod != "s-0-1" || h.c.Status.LastFailoverTime == nil {
		t.Fatalf("failover not recorded: %+v, %v", h.c.Status.Shards[0], h.c.Status.LastFailoverTime)
	}
	var pod corev1.Pod
	_ = h.r.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "s-0-1"}, &pod)
	if pod.Labels[kividbv1alpha1.RoleLabel] != "master" {
		t.Fatalf("s-0-1 role label %q", pod.Labels[kividbv1alpha1.RoleLabel])
	}
}

func TestReplacedNodeIsRejoinedAndItsOldIDForgotten(t *testing.T) {
	h := newFormingHarness(t, 2, 1)
	for i := 0; i < 4; i++ {
		h.pass()
	}
	oldID := h.node("s-1-1").id

	// s-1-1's volume was replaced: same pod and IP, a new node that knows
	// nobody. The others still have the old node.
	delete(h.engine.nodes, oldID)
	fresh := h.engine.add(nodeID("s-1-1 again"), "10.0.1.2")
	for _, n := range h.engine.nodes {
		if n != fresh {
			n.seen[oldID] = "10.0.1.2"
		}
	}
	h.engine.nodes[oldID] = &fakeNode{id: oldID, ip: "", seen: map[string]string{}} // known, unreachable

	for i := 0; i < 4; i++ {
		h.pass()
	}
	if fresh.master != h.node("s-1-0").id {
		t.Fatalf("the new node follows %q, want shard 1's master", fresh.master)
	}
	for _, n := range h.engine.nodes {
		if n.id == oldID {
			continue
		}
		if _, still := n.seen[oldID]; still {
			t.Fatalf("node %s still knows the replaced node", n.id[:6])
		}
	}
	if h.c.Status.Phase != kividbv1alpha1.PhaseRunning {
		t.Fatalf("phase %s", h.c.Status.Phase)
	}
}

func TestNodeOwningSlotsIsNeverForgotten(t *testing.T) {
	h := newFormingHarness(t, 2, 0)
	for i := 0; i < 4; i++ {
		h.pass()
	}
	// s-1-0 rejoined with a new ID while its old ID still owns shard 1's
	// slots (no replica took over): forgetting it would drop the slots.
	old := h.node("s-1-0")
	delete(h.engine.nodes, old.id)
	h.engine.add(nodeID("s-1-0 again"), "10.0.1.1")
	h.engine.nodes[old.id] = &fakeNode{id: old.id, slots: old.slots, seen: map[string]string{}}
	for i := 0; i < 3; i++ {
		h.pass()
	}
	for _, call := range h.engine.calls {
		if strings.HasPrefix(call, "/cluster/forget") && strings.Contains(call, old.id) {
			t.Fatalf("forgot a node that owns slots: %v", h.engine.calls)
		}
	}
}

func TestColdRestartReintroducesEveryNode(t *testing.T) {
	h := newFormingHarness(t, 2, 1)
	for i := 0; i < 4; i++ {
		h.pass()
	}
	// Every pod comes back with a new IP; the nodes still have each other
	// at the old ones.
	ctx := context.Background()
	for _, p := range h.pods() {
		n := h.node(p.Name)
		newIP := strings.Replace(n.ip, "10.0.", "10.9.", 1)
		delete(h.engine.byIP, n.ip)
		n.ip = newIP
		h.engine.byIP[newIP] = n.id
		pod := p.DeepCopy()
		pod.Status.PodIP = newIP
		if err := h.r.Status().Update(ctx, pod); err != nil {
			_ = h.r.Update(ctx, pod)
		}
	}
	for i := 0; i < 3; i++ {
		h.pass()
	}
	anchor := h.node("s-0-0")
	for id, ip := range anchor.seen {
		if !strings.HasPrefix(ip, "10.9.") {
			t.Fatalf("the anchor still has node %s at %s", id[:6], ip)
		}
	}
}
