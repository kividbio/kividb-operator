package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"testing"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	"github.com/kividbio/kividb-operator/internal/agentapi"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestBumpAuthGenerations(t *testing.T) {
	t.Parallel()
	secret := &corev1.Secret{}

	c := &kividbv1alpha1.KividbCluster{}
	first := bumpAuthGenerations(c, secret, "acl v1", "pw1")
	if first.auth != "1" || first.acl != "1" {
		t.Fatalf("first call: %+v, want both generations at 1", first)
	}
	if again := bumpAuthGenerations(c, secret, "acl v1", "pw1"); again != first {
		t.Fatalf("unchanged input moved the generations: %+v", again)
	}
	if got := bumpAuthGenerations(c, secret, "acl v2", "pw1"); got.auth != "1" || got.acl != "2" {
		t.Fatalf("ACL-only change: %+v, want auth=1 acl=2", got)
	}
	if got := bumpAuthGenerations(c, secret, "acl v3", "pw2"); got.auth != "2" || got.acl != "3" {
		t.Fatalf("default password change: %+v, want auth=2 acl=3", got)
	}
}

// aclAgents answers POST /acl/reload the way the agent does: 409 until the
// file "mounted" in that pod matches the hash the caller expects.
type aclAgents struct {
	fileHash map[string]string // pod IP -> hash of the ACL file in the pod
	reloaded []string
}

func (f *aclAgents) RoundTrip(req *http.Request) (*http.Response, error) {
	ip, _, _ := net.SplitHostPort(req.URL.Host)
	var body agentapi.AclReloadRequest
	_ = json.NewDecoder(req.Body).Decode(&body)

	status, out := http.StatusOK, any(agentapi.OKResponse{OK: true})
	if f.fileHash[ip] != body.IfFileHash {
		status, out = http.StatusConflict, agentapi.ErrorResponse{Error: "not yet"}
	} else {
		f.reloaded = append(f.reloaded, ip)
	}
	b, _ := json.Marshal(out)
	return &http.Response{StatusCode: status, Body: io.NopCloser(bytes.NewReader(b)), Header: http.Header{}}, nil
}

func TestReconcileAclReload(t *testing.T) {
	const acl = "user default reset on nopass ~* &* +@all\n"
	generations := authGenerations{auth: "2", acl: "5"}

	pod := func(name, ip string, annotations map[string]string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Annotations: annotations},
			Status: corev1.PodStatus{
				PodIP:      ip,
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			},
		}
	}
	pods := []client.Object{
		// Has the new file and the current default password: reload.
		pod("synced", "10.0.0.1", map[string]string{AuthGenerationAnnotation: "2", AclGenerationAnnotation: "4"}),
		// The updated Secret has not reached this pod's volume yet: wait.
		pod("stale-volume", "10.0.0.2", map[string]string{AuthGenerationAnnotation: "2", AclGenerationAnnotation: "4"}),
		// Still running with the previous default password: leave it to
		// the rolling restart, reloading would lock its agent out.
		pod("old-password", "10.0.0.3", map[string]string{AuthGenerationAnnotation: "1", AclGenerationAnnotation: "4"}),
		// Already loaded this generation: nothing to do.
		pod("done", "10.0.0.4", map[string]string{AuthGenerationAnnotation: "2", AclGenerationAnnotation: "5"}),
	}

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	agents := &aclAgents{fileHash: map[string]string{
		"10.0.0.1": aclFileFingerprint(acl),
		"10.0.0.2": aclFileFingerprint("the previous file"),
		"10.0.0.3": aclFileFingerprint(acl),
		"10.0.0.4": aclFileFingerprint(acl),
	}}
	r := &KividbClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(pods...).Build(),
		Agent:  &AgentClient{http: &http.Client{Transport: agents}},
	}

	var list corev1.PodList
	if err := r.List(context.Background(), &list); err != nil {
		t.Fatal(err)
	}
	r.reconcileAclReload(context.Background(), &kividbv1alpha1.KividbCluster{}, list.Items, acl, generations)

	if len(agents.reloaded) != 1 || agents.reloaded[0] != "10.0.0.1" {
		t.Errorf("reloaded %v, want only 10.0.0.1", agents.reloaded)
	}
	want := map[string]string{"synced": "5", "stale-volume": "4", "old-password": "4", "done": "5"}
	for name, generation := range want {
		var p corev1.Pod
		if err := r.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: name}, &p); err != nil {
			t.Fatal(err)
		}
		if got := p.Annotations[AclGenerationAnnotation]; got != generation {
			t.Errorf("pod %s: ACL generation %q, want %q", name, got, generation)
		}
	}
}
