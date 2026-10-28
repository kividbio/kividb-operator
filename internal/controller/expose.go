package controller

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// Access to a sharded cluster from outside Kubernetes. The operator never
// creates a load balancer: users expose a pod with a Service of their own
// (a LoadBalancer, a NodePort, ...) selecting it by
// statefulset.kubernetes.io/pod-name, and label that Service
// kividb.io/announce=true. The operator then has the pod's node announce
// the Service's address to clients, so a client outside follows MOVED to
// an address it can reach.

const (
	// AnnounceLabel marks a user's Service whose address a node announces.
	AnnounceLabel = "kividb.io/announce"
	// AnnounceAddressAnnotation sets the address to announce, for a
	// Service with no load-balancer address of its own (a NodePort behind
	// a known address, an ingress controller's TCP route): host or
	// host:port.
	AnnounceAddressAnnotation = "kividb.io/announce-address"
)

// exposure is the address a pod is announced at, or why it is not.
type exposure struct {
	service string
	address string // "" while the Service has none yet
	problem string
}

// announceHost checks that addr can be announced: a hostname or an IPv4
// address, on the cluster's client port. A node announces one port to
// clients and peers alike (kividbio/kividb#110). Engine 1.2 has
// cluster-announce-client-port, a port for clients only, which would allow
// another one here; the operator does not set it yet.
func announceHost(addr string, clientPort int32) (string, error) {
	host, port := addr, ""
	if h, p, err := net.SplitHostPort(addr); err == nil {
		host, port = h, p
	}
	if port != "" && port != strconv.Itoa(int(clientPort)) {
		return "", fmt.Errorf("port %s differs from the client port %d: a node announces one port to clients and peers alike (kividbio/kividb#110)", port, clientPort)
	}
	if host == "" || strings.Trim(strings.ToLower(host), "abcdefghijklmnopqrstuvwxyz0123456789-.") != "" {
		return "", fmt.Errorf("%q cannot be announced: a hostname or an IPv4 address only", host)
	}
	return host, nil
}

// exposures maps each of the cluster's pods that a user exposed to its
// announced address.
func (r *KividbClusterReconciler) exposures(ctx context.Context, c *kividbv1alpha1.KividbCluster) (map[string]exposure, error) {
	var list corev1.ServiceList
	if err := r.List(ctx, &list, client.InNamespace(c.Namespace), client.MatchingLabels{AnnounceLabel: "true"}); err != nil {
		return nil, err
	}
	pods := map[string]bool{}
	for s := int32(0); s < shardCount(c); s++ {
		for n := int32(0); n <= c.Spec.Replicas; n++ {
			pods[shardPodName(c, s, n)] = true
		}
	}
	out := map[string]exposure{}
	port := getPort(c)
	for i := range list.Items {
		svc := &list.Items[i]
		pod := svc.Spec.Selector[podNameLabel]
		if !pods[pod] {
			continue // another cluster's pod, or no pod selector
		}
		e := exposure{service: svc.Name}
		hasPort := svc.Annotations[AnnounceAddressAnnotation] != ""
		for _, p := range svc.Spec.Ports {
			if p.Port == port {
				hasPort = true
			}
		}
		addr := svc.Annotations[AnnounceAddressAnnotation]
		if addr == "" {
			for _, ing := range svc.Status.LoadBalancer.Ingress {
				if ing.Hostname != "" {
					addr = ing.Hostname
					break
				}
				if ing.IP != "" {
					addr = ing.IP
					break
				}
			}
		}
		switch {
		case !hasPort:
			e.problem = fmt.Sprintf("Service %s has no port %d: clients would be sent to a port it does not serve", svc.Name, port)
		case addr != "":
			if host, err := announceHost(addr, port); err != nil {
				e.problem = fmt.Sprintf("Service %s: %v", svc.Name, err)
			} else {
				e.address = host
			}
		}
		if prev, dup := out[pod]; dup {
			e.problem = fmt.Sprintf("pod %s is exposed by both %s and %s; label one of them", pod, prev.service, svc.Name)
			e.address = ""
		}
		out[pod] = e
	}
	return out, nil
}

// exposureCondition reports on outside access: all of the cluster's pods
// exposed, none, or some (which leaves outside clients sent to names they
// cannot reach), and any problems.
func exposureCondition(c *kividbv1alpha1.KividbCluster, exp map[string]exposure) *metav1.Condition {
	if len(exp) == 0 {
		return nil
	}
	total := int(shardCount(c) * (c.Spec.Replicas + 1))
	var problems, waiting []string
	ready := 0
	for pod, e := range exp {
		switch {
		case e.problem != "":
			problems = append(problems, e.problem)
		case e.address == "":
			waiting = append(waiting, pod)
		default:
			ready++
		}
	}
	sort.Strings(problems)
	sort.Strings(waiting)
	cond := &metav1.Condition{Type: "ExternallyAccessible", Status: metav1.ConditionFalse, ObservedGeneration: c.Generation}
	switch {
	case len(problems) > 0:
		cond.Reason, cond.Message = "AnnounceServiceProblem", strings.Join(problems, "; ")
	case len(waiting) > 0:
		cond.Reason, cond.Message = "WaitingForAddress", "no address yet for "+strings.Join(waiting, ", ")
	case ready < total:
		cond.Reason = "PartiallyExposed"
		cond.Message = fmt.Sprintf("%d of %d pods are exposed: clients outside Kubernetes are sent to every node, and cannot reach the others", ready, total)
	default:
		cond.Status, cond.Reason = metav1.ConditionTrue, "AllPodsExposed"
		cond.Message = fmt.Sprintf("all %d pods announce an outside address", total)
	}
	return cond
}

// mapAnnounceServiceToCluster enqueues the cluster whose pod a labelled
// Service selects, so a load balancer getting its address is announced
// without waiting for the periodic reconcile. A shard pod is named
// <cluster>-<shard>-<n>.
func mapAnnounceServiceToCluster(_ context.Context, obj client.Object) []reconcile.Request {
	svc, ok := obj.(*corev1.Service)
	if !ok || svc.Labels[AnnounceLabel] != "true" {
		return nil
	}
	name := svc.Spec.Selector[podNameLabel]
	for range 2 {
		i := strings.LastIndexByte(name, '-')
		if i <= 0 {
			return nil
		}
		name = name[:i]
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: svc.Namespace, Name: name}}}
}

// announceTarget is the name a pod's node announces: its exposed address,
// or its DNS name.
func announceTarget(c *kividbv1alpha1.KividbCluster, p *shardPod, exp map[string]exposure) string {
	if e, ok := exp[p.pod.Name]; ok && e.address != "" {
		return e.address
	}
	return announceName(c, p)
}
