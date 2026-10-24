package controller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	"github.com/kividbio/kividb-operator/internal/agentapi"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestEnsureAgentTokenCreatesOnceAndKeepsIt(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	ctx := context.Background()

	first, err := EnsureAgentToken(ctx, c, "ops", "agent-token")
	if err != nil || len(first) != 64 {
		t.Fatalf("first: %q, %v", first, err)
	}
	again, err := EnsureAgentToken(ctx, c, "ops", "agent-token")
	if err != nil || again != first {
		t.Fatalf("a second start made a new token: %q vs %q (%v)", again, first, err)
	}
	var s corev1.Secret
	if err := c.Get(ctx, client.ObjectKey{Namespace: "ops", Name: "agent-token"}, &s); err != nil || string(s.Data[agentapi.TokenSecretKey]) != first {
		t.Fatalf("Secret: %v %v", s.Data, err)
	}

	// A Secret someone else made, with the key missing, is an error, not a
	// silent empty token.
	bad := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "ops", Name: "empty"}}
	if err := c.Create(ctx, bad); err != nil {
		t.Fatal(err)
	}
	if _, err := EnsureAgentToken(ctx, c, "ops", "empty"); err == nil {
		t.Fatal("accepted a Secret without the token")
	}
}

func TestAgentClientSendsTheToken(t *testing.T) {
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = append(got, r.URL.Path+" "+r.Header.Get("Authorization"))
		_, _ = w.Write([]byte(`{"role":"master"}`))
	}))
	defer srv.Close()

	a := NewAgentClient(time.Second, "s3cret")
	a.http.Transport = rewriteTo(srv.URL)
	if _, err := a.Status(context.Background(), "10.0.0.1"); err != nil {
		t.Fatal(err)
	}
	if err := a.Promote(context.Background(), "10.0.0.1"); err != nil {
		t.Fatal(err)
	}
	for _, g := range got {
		if !strings.HasSuffix(g, " Bearer s3cret") {
			t.Errorf("request without the token: %q", g)
		}
	}
	if len(got) != 2 {
		t.Fatalf("requests: %v", got)
	}
}

// rewriteTo sends every request to base, keeping its path.
func rewriteTo(base string) http.RoundTripper {
	return roundTripFunc(func(r *http.Request) (*http.Response, error) {
		u := *r.URL
		u.Scheme = "http"
		u.Host = strings.TrimPrefix(base, "http://")
		r2 := r.Clone(r.Context())
		r2.URL = &u
		return http.DefaultTransport.RoundTrip(r2)
	})
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestTheTokenReachesTheAgentsAndTheAuthSecret(t *testing.T) {
	c := &kividbv1alpha1.KividbCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "prod"}}

	s := desiredAuthSecret(c, "user default on nopass", "tok")
	if s.StringData[agentapi.TokenSecretKey] != "tok" {
		t.Fatalf("auth Secret: %v", s.StringData)
	}

	var found bool
	for _, e := range agentEnv(c, nil, nil) {
		if e.Name == agentapi.TokenEnv {
			found = e.ValueFrom != nil && e.ValueFrom.SecretKeyRef.Name == "demo-auth" && e.ValueFrom.SecretKeyRef.Key == agentapi.TokenSecretKey
		}
	}
	if !found {
		t.Fatal("the agent's environment has no AGENT_TOKEN from the auth Secret")
	}

	// A new token restarts the pods, as a new password does.
	secret := &corev1.Secret{}
	first := bumpAuthGenerations(c, secret, "acl", "pw", "tok")
	if got := bumpAuthGenerations(c, secret, "acl", "pw", "new-tok"); got.auth == first.auth {
		t.Fatal("a new agent token did not bump the auth generation")
	}
}

func TestOnlyTheHeadlessServiceHasTheAgentPort(t *testing.T) {
	c := &kividbv1alpha1.KividbCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "prod"}}
	hasAgent := func(s *corev1.Service) bool {
		for _, p := range s.Spec.Ports {
			if p.Port == AgentPort || p.Name == AgentPortName {
				return true
			}
		}
		return false
	}
	headless := desiredHeadlessService(c, nil)
	if !hasAgent(headless) || headless.Labels[ServiceKindLabel] != "headless" {
		t.Fatalf("headless Service: ports %v labels %v", headless.Spec.Ports, headless.Labels)
	}
	for _, s := range []*corev1.Service{desiredMasterService(c, nil), desiredReplicaService(c, nil)} {
		if hasAgent(s) {
			t.Errorf("%s exposes the agent port: %v", s.Name, s.Spec.Ports)
		}
		if s.Labels[ServiceKindLabel] == "headless" {
			t.Errorf("%s would be scraped as the headless Service", s.Name)
		}
	}
}

func TestClusterEnabledIsRefused(t *testing.T) {
	cfg := func(d map[string]string) *kividbv1alpha1.KividbConfig {
		return &kividbv1alpha1.KividbConfig{ObjectMeta: metav1.ObjectMeta{Name: "cfg"}, Spec: kividbv1alpha1.KividbConfigSpec{Directives: d}}
	}
	for _, d := range []map[string]string{
		{"cluster-enabled": "yes"},
		{"Cluster-Enabled": "YES"},
		{"cluster-enabled": "true"},
	} {
		if validateKividbConfig(cfg(d)) == nil {
			t.Errorf("accepted %v", d)
		}
	}
	for _, d := range []map[string]string{
		nil,
		{"cluster-enabled": "no"},
		{"cluster-enabled": " No "},
		{"maxmemory": "1gb"},
	} {
		if err := validateKividbConfig(cfg(d)); err != nil {
			t.Errorf("refused %v: %v", d, err)
		}
	}
	if validateKividbConfig(nil) != nil {
		t.Error("refused no KividbConfig")
	}
}
