package controller

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	"github.com/kividbio/kividb-operator/internal/agentapi"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestUnionCredentials(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct{ a, b, want string }{
		{"#bb", "#aa", "#aa #bb"},
		{"#aa", "#aa", "#aa"},
		{"nopass", "#aa", "nopass"},
		{"#aa", "nopass", "nopass"},
		{"#aa", "", "#aa"},
	} {
		if got := strings.Join(unionCredentials(strings.Fields(tt.a), strings.Fields(tt.b)), " "); got != tt.want {
			t.Errorf("union(%q, %q) = %q, want %q", tt.a, tt.b, got, tt.want)
		}
	}
}

// passwordCluster is a replicated cluster whose default user's password
// is in Secret pw/password, and a reconciler around it.
func passwordCluster(t *testing.T) (*KividbClusterReconciler, *kividbv1alpha1.KividbCluster, *kividbv1alpha1.KividbAclConfig) {
	t.Helper()
	c := &kividbv1alpha1.KividbCluster{ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "default", UID: "uid"}, Spec: kividbv1alpha1.KividbClusterSpec{Replicas: 1}}
	acl := &kividbv1alpha1.KividbAclConfig{Spec: kividbv1alpha1.KividbAclConfigSpec{RequirePassSecretRef: &kividbv1alpha1.SecretKeyRef{Name: "pw", Key: "password"}}}
	return shardedReconciler(t, c), c, acl
}

// secretPass renders and writes the auth Secret for password ("" for none).
func secretPass(t *testing.T, r *KividbClusterReconciler, c *kividbv1alpha1.KividbCluster, acl *kividbv1alpha1.KividbAclConfig, password string) (authGenerations, string) {
	t.Helper()
	cfg, values := acl, map[string]string{"pw/password": password}
	if password == "" {
		cfg, values = nil, nil
	}
	base, err := renderACLFile(cfg, values)
	if err != nil {
		t.Fatal(err)
	}
	g, content, err := r.reconcileSecret(context.Background(), c, cfg, values, base, password)
	if err != nil {
		t.Fatal(err)
	}
	return g, content
}

func TestPasswordChangeKeepsTheOldPasswordUntilTheRolloutIsOver(t *testing.T) {
	r, c, acl := passwordCluster(t)
	h1, h2 := "#"+hashPassword("pw-one"), "#"+hashPassword("pw-two")

	g, content := secretPass(t, r, c, acl, "pw-one")
	if len(g.overlap) != 0 || !strings.Contains(content, "user default reset on "+h1+" ~*") {
		t.Fatalf("first password: overlap %v, file %q", g.overlap, content)
	}

	g, content = secretPass(t, r, c, acl, "pw-two")
	both := strings.Join(unionCredentials([]string{h1}, []string{h2}), " ")
	if fmt.Sprint(g.overlap) != "["+h1+"]" || !strings.Contains(content, "user default reset on "+both+" ~*") {
		t.Fatalf("changed password: overlap %v, file %q", g.overlap, content)
	}
	// It stays while the rollout runs.
	if g2, content2 := secretPass(t, r, c, acl, "pw-two"); fmt.Sprint(g2.overlap) != fmt.Sprint(g.overlap) || content2 != content || g2.auth != g.auth {
		t.Fatalf("a later pass changed the overlap: %v %q", g2.overlap, content2)
	}

	// The rollout is over: the previous password goes.
	r.endAuthOverlap(context.Background(), c)
	g, content = secretPass(t, r, c, acl, "pw-two")
	if len(g.overlap) != 0 || !strings.Contains(content, "user default reset on "+h2+" ~*") {
		t.Fatalf("after the rollout: overlap %v, file %q", g.overlap, content)
	}
}

// Removing the password: the new pods are open (nopass) and have no
// masterauth; the old ones must accept them until they are replaced, so
// they are opened live too (A4b: replicas could not authenticate to the
// old master, and 13,455 acknowledged writes were lost).
func TestPasswordRemovalOpensTheOldPodsFirst(t *testing.T) {
	r, c, acl := passwordCluster(t)
	secretPass(t, r, c, acl, "pw-one")
	g, content := secretPass(t, r, c, nil, "")
	if len(g.overlap) == 0 || fmt.Sprint(g.defaultCredentials) != "[nopass]" || !strings.Contains(content, "user default reset on nopass") {
		t.Fatalf("removal: overlap %v, credentials %v, file %q", g.overlap, g.defaultCredentials, content)
	}
}

// Setting a password where there was none: open until every pod has it.
func TestPasswordSetStaysOpenDuringTheRollout(t *testing.T) {
	r, c, acl := passwordCluster(t)
	secretPass(t, r, c, nil, "")
	g, content := secretPass(t, r, c, acl, "pw-three")
	if fmt.Sprint(g.overlap) != "[nopass]" || !strings.Contains(content, "user default reset on nopass") {
		t.Fatalf("set: overlap %v, file %q", g.overlap, content)
	}
}

func TestReconcileAuthOverlapWidensThePodsOnThePreviousPassword(t *testing.T) {
	r, c, _ := passwordCluster(t)
	ctx := context.Background()
	pod := func(name, ip, gen string, ready bool) *corev1.Pod {
		cond := corev1.ConditionTrue
		if !ready {
			cond = corev1.ConditionFalse
		}
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: selectorLabels(c), Annotations: map[string]string{AuthGenerationAnnotation: gen}},
			Status:     corev1.PodStatus{PodIP: ip, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: cond}}},
		}
	}
	for _, p := range []*corev1.Pod{pod("c1-0", "10.0.0.1", "1", true), pod("c1-1", "10.0.0.2", "2", true)} {
		if err := r.Create(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	agents := &fakeAgents{status: map[string]*agentapi.StatusResponse{"10.0.0.1": master(1), "10.0.0.2": replicaOf("10.0.0.1", 1)}}
	r.Agent = &AgentClient{http: &http.Client{Transport: agents}}
	g := authGenerations{auth: "2", overlap: []string{"#aa"}, defaultCredentials: []string{"#aa", "#bb"}}
	list := func() []corev1.Pod {
		var l corev1.PodList
		if err := r.List(ctx, &l); err != nil {
			t.Fatal(err)
		}
		return l.Items
	}

	if !r.reconcileAuthOverlap(ctx, c, list(), g) {
		t.Fatal("the rollout was held although every old pod was widened")
	}
	if fmt.Sprint(agents.calls) != "[exec 10.0.0.1 ACL SETUSER default #aa #bb]" {
		t.Fatalf("calls %v", agents.calls)
	}
	// Once only.
	agents.calls = nil
	r.reconcileAuthOverlap(ctx, c, list(), g)
	if len(agents.calls) != 0 {
		t.Fatalf("widened again: %v", agents.calls)
	}

	// The old pod is replaced: every pod is on generation 2, the overlap ends.
	var p corev1.Pod
	if err := r.Get(ctx, client.ObjectKey{Namespace: "default", Name: "c1-0"}, &p); err != nil {
		t.Fatal(err)
	}
	p.Annotations[AuthGenerationAnnotation] = "2"
	if err := r.Update(ctx, &p); err != nil {
		t.Fatal(err)
	}
	if err := r.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secretName(c), Namespace: "default"}, StringData: map[string]string{defaultOverlapKey: "#aa"}}); err != nil {
		t.Fatal(err)
	}
	r.reconcileAuthOverlap(ctx, c, list(), g)
	var s corev1.Secret
	if err := r.Get(ctx, client.ObjectKey{Namespace: "default", Name: secretName(c)}, &s); err != nil {
		t.Fatal(err)
	}
	if secretString(&s, defaultOverlapKey) != "" {
		t.Fatalf("overlap still %q after the rollout", secretString(&s, defaultOverlapKey))
	}
}

func TestReconcileAuthOverlapHoldsTheRolloutWhenAPodCannotBeWidened(t *testing.T) {
	r, c, _ := passwordCluster(t)
	ctx := context.Background()
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "c1-0", Namespace: "default", Annotations: map[string]string{AuthGenerationAnnotation: "1"}},
		Status:     corev1.PodStatus{PodIP: "10.0.0.1", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}
	// No agent at 10.0.0.1: the call fails.
	r.Agent = &AgentClient{http: &http.Client{Transport: &fakeAgents{status: map[string]*agentapi.StatusResponse{}}}}
	if r.reconcileAuthOverlap(ctx, c, []corev1.Pod{*p}, authGenerations{auth: "2", overlap: []string{"#aa"}, defaultCredentials: []string{"nopass"}}) {
		t.Fatal("the rollout may go on although the old pod still refuses the new password")
	}
}
