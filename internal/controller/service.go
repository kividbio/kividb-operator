package controller

import (
	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// desiredHeadlessService backs the StatefulSet's stable network identity
// (pod-name.<headless>.<ns>.svc.cluster.local). It selects every pod in the
// cluster regardless of role.
func desiredHeadlessService(c *kividbv1alpha1.KividbCluster, kdbConfig *kividbv1alpha1.KividbConfig) *corev1.Service {
	ports := []corev1.ServicePort{
		{Name: KividbPortName, Port: getPort(c), TargetPort: intstr.FromInt32(getPort(c))},
		{Name: AgentPortName, Port: AgentPort, TargetPort: intstr.FromInt32(AgentPort)},
	}
	if tls := tlsServicePort(kdbConfig); tls != nil {
		ports = append(ports, *tls)
	}
	if c.Spec.Monitoring.Enabled {
		ports = append(ports, corev1.ServicePort{Name: ExporterPortName, Port: ExporterPort, TargetPort: intstr.FromInt32(ExporterPort)})
	}

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      headlessServiceName(c),
			Namespace: c.Namespace,
			Labels:    commonLabels(c),
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			Selector:                 selectorLabels(c),
			PublishNotReadyAddresses: true,
			Ports:                    ports,
		},
	}
}

// desiredRoleService builds either the master or replica Service. Both
// select purely on kividbv1alpha1.RoleLabel, which the controller moves
// between pods on failover -- the Service objects themselves never change
// during a failover.
func desiredRoleService(c *kividbv1alpha1.KividbCluster, kdbConfig *kividbv1alpha1.KividbConfig, role kividbv1alpha1.NodeRole, name string, spec kividbv1alpha1.ServiceSpec) *corev1.Service {
	selector := selectorLabels(c)
	selector[kividbv1alpha1.RoleLabel] = string(role)

	svcType := spec.Type
	if svcType == "" {
		svcType = corev1.ServiceTypeClusterIP
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   c.Namespace,
			Labels:      commonLabels(c),
			Annotations: spec.Annotations,
		},
		Spec: corev1.ServiceSpec{
			Type:     svcType,
			Selector: selector,
			Ports: []corev1.ServicePort{
				{Name: KividbPortName, Port: getPort(c), TargetPort: intstr.FromInt32(getPort(c))},
				// Agent port must be reachable here too: the backup CronJob's
				// backup-trigger client POSTs to
				// http://<cluster>-master.<ns>.svc:<AgentPort>/backup (see
				// backup.go), and this Service is how it always finds
				// whichever pod currently holds the master role.
				{Name: AgentPortName, Port: AgentPort, TargetPort: intstr.FromInt32(AgentPort)},
			},
		},
	}

	if tls := tlsServicePort(kdbConfig); tls != nil {
		svc.Spec.Ports = append(svc.Spec.Ports, *tls)
	}

	if svcType == corev1.ServiceTypeLoadBalancer {
		svc.Spec.LoadBalancerIP = spec.LoadBalancerIP //nolint:staticcheck // still the simplest cross-provider way to request a static IP
		svc.Spec.LoadBalancerSourceRanges = spec.LoadBalancerSourceRanges
	}

	return svc
}

func desiredMasterService(c *kividbv1alpha1.KividbCluster, kdbConfig *kividbv1alpha1.KividbConfig) *corev1.Service {
	return desiredRoleService(c, kdbConfig, kividbv1alpha1.RoleMaster, masterServiceName(c), c.Spec.Services.Master)
}

func desiredReplicaService(c *kividbv1alpha1.KividbCluster, kdbConfig *kividbv1alpha1.KividbConfig) *corev1.Service {
	return desiredRoleService(c, kdbConfig, kividbv1alpha1.RoleReplica, replicaServiceName(c), c.Spec.Services.Replicas)
}

// tlsServicePort is the TLS listener's port on the Services, when the
// cluster's KividbConfig enables TLS. The pods always listened on it, but no
// Service carried it, so a TLS client could reach kividb only by pod IP.
func tlsServicePort(kdbConfig *kividbv1alpha1.KividbConfig) *corev1.ServicePort {
	tls := kividbTLSSpec(kdbConfig)
	if tls == nil || !tls.Enabled {
		return nil
	}
	port := tls.Port
	if port == 0 {
		port = 6443
	}
	return &corev1.ServicePort{Name: "tls", Port: port, TargetPort: intstr.FromInt32(port)}
}
