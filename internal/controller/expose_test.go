package controller

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func announceService(name, pod string, port int32, annotations map[string]string, ingress ...corev1.LoadBalancerIngress) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "default", Labels: map[string]string{AnnounceLabel: "true"}, Annotations: annotations,
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeLoadBalancer,
			Selector: map[string]string{podNameLabel: pod},
			Ports:    []corev1.ServicePort{{Port: port}},
		},
		Status: corev1.ServiceStatus{LoadBalancer: corev1.LoadBalancerStatus{Ingress: ingress}},
	}
}

func TestAnnounceHost(t *testing.T) {
	for addr, want := range map[string]string{
		"20.1.2.3":                   "20.1.2.3",
		"20.1.2.3:6379":              "20.1.2.3",
		"kv-0.example.com":           "kv-0.example.com",
		"KV-0.Example.com:6379":      "KV-0.Example.com",
		"a1b2.elb.amazonaws.com":     "a1b2.elb.amazonaws.com",
		"20.1.2.3:7000":              "",
		"[2001:db8::1]:6379":         "",
		"bad host":                   "",
		"":                           "",
		"kv.example.com\nCONFIG SET": "",
	} {
		got, err := announceHost(addr, 6379)
		if got != want || (want == "") != (err != nil) {
			t.Errorf("announceHost(%q) = %q, %v; want %q", addr, got, err, want)
		}
	}
}

func TestExposures(t *testing.T) {
	c := shardedCluster("s", 2, 1)
	c.Spec.Port = 6379
	other := announceService("other-cluster", "t-0-0", 6379, nil, corev1.LoadBalancerIngress{IP: "20.0.0.9"})
	r := shardedReconciler(t,
		announceService("lb-s-0-0", "s-0-0", 6379, nil, corev1.LoadBalancerIngress{IP: "20.0.0.1"}),
		announceService("lb-s-0-1", "s-0-1", 6379, nil, corev1.LoadBalancerIngress{Hostname: "s-0-1.lb.example.com"}),
		announceService("lb-s-1-0", "s-1-0", 6379, nil), // no address yet
		announceService("lb-s-1-1", "s-1-1", 7000, nil, corev1.LoadBalancerIngress{IP: "20.0.0.4"}),
		other,
	)
	exp, err := r.exposures(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if len(exp) != 4 {
		t.Fatalf("exposures %+v", exp)
	}
	if exp["s-0-0"].address != "20.0.0.1" || exp["s-0-1"].address != "s-0-1.lb.example.com" {
		t.Fatalf("addresses %+v", exp)
	}
	if exp["s-1-0"].address != "" || exp["s-1-0"].problem != "" {
		t.Fatalf("waiting pod %+v", exp["s-1-0"])
	}
	if !strings.Contains(exp["s-1-1"].problem, "no port 6379") {
		t.Fatalf("wrong port %+v", exp["s-1-1"])
	}

	cond := exposureCondition(c, exp)
	if cond.Status != metav1.ConditionFalse || cond.Reason != "AnnounceServiceProblem" {
		t.Fatalf("condition %+v", cond)
	}
	delete(exp, "s-1-1")
	if cond := exposureCondition(c, exp); cond.Reason != "WaitingForAddress" || !strings.Contains(cond.Message, "s-1-0") {
		t.Fatalf("condition %+v", cond)
	}
	exp["s-1-0"] = exposure{service: "lb-s-1-0", address: "20.0.0.3"}
	if cond := exposureCondition(c, exp); cond.Reason != "PartiallyExposed" || !strings.HasPrefix(cond.Message, "3 of 4") {
		t.Fatalf("condition %+v", cond)
	}
	exp["s-1-1"] = exposure{service: "lb-s-1-1", address: "20.0.0.4"}
	if cond := exposureCondition(c, exp); cond.Status != metav1.ConditionTrue || cond.Reason != "AllPodsExposed" {
		t.Fatalf("condition %+v", cond)
	}
	if exposureCondition(c, nil) != nil {
		t.Fatal("a condition with nothing exposed")
	}
}

func TestExposuresAnnotationAndDuplicates(t *testing.T) {
	c := shardedCluster("s", 1, 0)
	c.Spec.Port = 6379
	// A NodePort behind a known address: the annotation is announced, and
	// the Service's own ports are not checked against the client port.
	np := announceService("np-s-0-0", "s-0-0", 31000, map[string]string{AnnounceAddressAnnotation: "kv.example.com:6379"})
	np.Spec.Type = corev1.ServiceTypeNodePort
	r := shardedReconciler(t, np)
	exp, err := r.exposures(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if exp["s-0-0"].address != "kv.example.com" {
		t.Fatalf("annotation: %+v", exp)
	}

	r = shardedReconciler(t, np, announceService("lb-s-0-0", "s-0-0", 6379, nil, corev1.LoadBalancerIngress{IP: "20.0.0.1"}))
	exp, _ = r.exposures(context.Background(), c)
	if e := exp["s-0-0"]; e.address != "" || !strings.Contains(e.problem, "exposed by both") {
		t.Fatalf("duplicate: %+v", e)
	}
}

func TestMapAnnounceServiceToCluster(t *testing.T) {
	svc := announceService("lb", "my-cache-2-1", 6379, nil)
	got := mapAnnounceServiceToCluster(context.Background(), svc)
	if len(got) != 1 || got[0].Name != "my-cache" || got[0].Namespace != "default" {
		t.Fatalf("%v", got)
	}
	delete(svc.Labels, AnnounceLabel)
	if got := mapAnnounceServiceToCluster(context.Background(), svc); got != nil {
		t.Fatalf("unlabelled Service mapped: %v", got)
	}
	svc = announceService("lb", "nodash", 6379, nil)
	if got := mapAnnounceServiceToCluster(context.Background(), svc); got != nil {
		t.Fatalf("%v", got)
	}
}

func TestExposedPodAnnouncesItsAddress(t *testing.T) {
	h := newFormingHarness(t, 2, 0)
	h.exposed = map[string]exposure{
		"s-0-0": {service: "lb-s-0-0", address: "20.0.0.1"},
		"s-1-0": {service: "lb-s-1-0"}, // waiting for its address
	}
	for i := 0; i < 3; i++ {
		h.pass()
	}
	if got := h.node("s-0-0").hostname; got != "20.0.0.1" {
		t.Fatalf("s-0-0 announces %q", got)
	}
	if got := h.node("s-1-0").hostname; got != "s-1-0.s-1-headless.default.svc" {
		t.Fatalf("s-1-0 announces %q", got)
	}

	// The address arrives; then the Service is removed again.
	h.exposed["s-1-0"] = exposure{service: "lb-s-1-0", address: "lb-1.example.com"}
	h.pass()
	if got := h.node("s-1-0").hostname; got != "lb-1.example.com" {
		t.Fatalf("s-1-0 announces %q", got)
	}
	h.exposed = nil
	h.pass()
	if got := h.node("s-0-0").hostname; got != "s-0-0.s-0-headless.default.svc" {
		t.Fatalf("s-0-0 announces %q after its Service went", got)
	}
}
