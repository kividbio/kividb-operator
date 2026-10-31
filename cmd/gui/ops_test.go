package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Promoting a replica of a replicated cluster asks the operator for a
// switchover, which loses nothing; it no longer promotes the pod itself.
func TestPromoteAsksTheOperatorForASwitchover(t *testing.T) {
	c := &kividbv1alpha1.KividbCluster{ObjectMeta: metav1.ObjectMeta{Name: "rep", Namespace: "prod"}, Spec: kividbv1alpha1.KividbClusterSpec{Replicas: 1}}
	pod := func(name, role string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "prod", Labels: map[string]string{
				kividbv1alpha1.ClusterLabel: "rep", appNameLabel: appNameValue, kividbv1alpha1.RoleLabel: role,
			}},
			Status: corev1.PodStatus{PodIP: "10.0.0." + name[len(name)-1:]},
		}
	}
	s := guiServer(t, []client.Object{c}, pod("rep-0", "master"), pod("rep-1", "replica"))
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"pod":"rep-1"}`))
	req.SetPathValue("namespace", "prod")
	req.SetPathValue("name", "rep")
	rec := httptest.NewRecorder()
	s.handleAPIPromote(rec, req)
	if rec.Code != http.StatusAccepted || !strings.Contains(rec.Body.String(), `"requested":"rep-1"`) {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
	var got kividbv1alpha1.KividbCluster
	_ = s.ctrlClient.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "rep"}, &got)
	if got.Annotations[switchoverToAnnotation] != "rep-1" {
		t.Fatalf("annotations %v", got.Annotations)
	}
}

// The DbOps list goes by the DbOps' cluster reference: one created with
// kubectl has no kividb.io/cluster label and is listed all the same.
func TestListDbOpsByClusterReference(t *testing.T) {
	op := func(name, cluster string, labels map[string]string) *kividbv1alpha1.KividbDbOps {
		return &kividbv1alpha1.KividbDbOps{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "prod", Labels: labels},
			Spec:       kividbv1alpha1.KividbDbOpsSpec{ClusterRef: corev1.LocalObjectReference{Name: cluster}, Op: kividbv1alpha1.DbOpsRestart},
		}
	}
	s := guiServer(t, []client.Object{
		op("rep-restart-1", "rep", nil),
		op("rep-restart-gui", "rep", map[string]string{kividbv1alpha1.ClusterLabel: "rep"}),
		op("other-restart", "other", nil),
	})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetPathValue("namespace", "prod")
	req.SetPathValue("name", "rep")
	rec := httptest.NewRecorder()
	s.handleAPIListDbOps(rec, req)
	var got []kividbv1alpha1.KividbDbOps
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("%d %s: %v", rec.Code, rec.Body, err)
	}
	var names []string
	for _, o := range got {
		names = append(names, o.Name)
	}
	sort.Strings(names)
	if strings.Join(names, " ") != "rep-restart-1 rep-restart-gui" {
		t.Fatalf("listed %v", names)
	}
}

// Promoting a shard's replica asks the engine for CLUSTER FAILOVER and
// records a ShardSwitchover Event on the cluster, as the operator does for
// the handovers it asks for.
func TestShardPromoteRecordsAnEvent(t *testing.T) {
	c := shardedFixture()
	pod := func(name, shard, role string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "prod", Labels: map[string]string{
				kividbv1alpha1.ClusterLabel: "cache", appNameLabel: appNameValue, kividbv1alpha1.RoleLabel: role, shardLabel: shard,
			}},
			Status: corev1.PodStatus{PodIP: "10.0.0." + name[len(name)-1:]},
		}
	}
	s := guiServer(t, []client.Object{c}, pod("cache-0-0", "0", "master"), pod("cache-0-1", "0", "replica"))
	var calls []string
	s.agentPost = func(_ context.Context, ip, path string, _ any) ([]byte, error) {
		calls = append(calls, ip+path)
		return []byte(`{"ok":true}`), nil
	}
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"pod":"cache-0-1"}`))
	req.SetPathValue("namespace", "prod")
	req.SetPathValue("name", "cache")
	rec := httptest.NewRecorder()
	s.handleAPIPromote(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
	if strings.Join(calls, " ") != "10.0.0.1/cluster/failover" {
		t.Fatalf("agent calls %v", calls)
	}
	events, err := s.clientset.CoreV1().Events("prod").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(events.Items) != 1 {
		t.Fatalf("events %+v", events.Items)
	}
	e := events.Items[0]
	if e.Reason != "ShardSwitchover" || e.Type != corev1.EventTypeNormal || e.InvolvedObject.Kind != "KividbCluster" || e.InvolvedObject.Name != "cache" ||
		e.Message != "shard 0: asked cache-0-1 to take over from cache-0-0 (CLUSTER FAILOVER), from the GUI" {
		t.Fatalf("event %+v", e)
	}
	if got := fetchClusterEvents(context.Background(), s.clientset, "prod", "cache"); len(got) != 1 {
		t.Fatalf("the cluster view shows %d events", len(got))
	}
	// Noted on the cluster, so the operator reports the move as asked for,
	// not as a Warning ShardFailover.
	var got kividbv1alpha1.KividbCluster
	if err := s.ctrlClient.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "cache"}, &got); err != nil {
		t.Fatal(err)
	}
	pod0, at, _ := strings.Cut(got.Annotations[kividbv1alpha1.ShardPromoteAnnotation], " ")
	if when, err := time.Parse(time.RFC3339, at); pod0 != "cache-0-1" || err != nil || time.Since(when) > time.Minute {
		t.Fatalf("annotation %q", got.Annotations[kividbv1alpha1.ShardPromoteAnnotation])
	}
}
