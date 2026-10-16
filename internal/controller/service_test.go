package controller

import (
	"testing"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

func servicePort(svc *corev1.Service, name string) *corev1.ServicePort {
	for i := range svc.Spec.Ports {
		if svc.Spec.Ports[i].Name == name {
			return &svc.Spec.Ports[i]
		}
	}
	return nil
}

// With TLS on, every Service carries the TLS port: clients could otherwise
// reach the TLS listener only by pod IP.
func TestServicesCarryTheTLSPort(t *testing.T) {
	t.Parallel()
	c := testCluster("demo")
	tls := &kividbv1alpha1.KividbConfig{
		Spec: kividbv1alpha1.KividbConfigSpec{
			TLS: &kividbv1alpha1.KividbTLSSpec{Enabled: true, Port: 6444},
		},
	}
	for _, svc := range []*corev1.Service{
		desiredHeadlessService(c, tls), desiredMasterService(c, tls), desiredReplicaService(c, tls),
	} {
		p := servicePort(svc, "tls")
		if p == nil || p.Port != 6444 || p.TargetPort.IntValue() != 6444 {
			t.Fatalf("%s: want a tls port 6444, got %+v", svc.Name, svc.Spec.Ports)
		}
	}
	for _, svc := range []*corev1.Service{
		desiredHeadlessService(c, nil), desiredMasterService(c, nil), desiredReplicaService(c, nil),
	} {
		if servicePort(svc, "tls") != nil {
			t.Fatalf("%s: no TLS, no tls port, got %+v", svc.Name, svc.Spec.Ports)
		}
	}
}
