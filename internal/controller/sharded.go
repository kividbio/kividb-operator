package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// A sharded KividbCluster (spec.shards set) runs kividb in cluster mode:
// one StatefulSet per shard, <cluster>-<shard>, whose pods
// <cluster>-<shard>-<n> are that shard's master and replicas.

const (
	// ShardLabel carries a pod's (and its Services') shard index.
	ShardLabel = "kividb.io/shard"

	// podNameLabel is set by the StatefulSet controller on every pod.
	podNameLabel = "statefulset.kubernetes.io/pod-name"

	// maxShardedNodes caps shards × (replicas + 1) until larger clusters
	// are tested (kividbio/kividb#112).
	maxShardedNodes = 24

	// maxShardedNameLen leaves room for -<shard>-<n>, -<shard>-headless and
	// the StatefulSet's controller-revision-hash label within the
	// Kubernetes limits.
	maxShardedNameLen = 40

	defaultNodeTimeoutMs  = 5000
	defaultMinReplicasLag = 10

	busSecretKey = "secret"
)

func isSharded(c *kividbv1alpha1.KividbCluster) bool { return c.Spec.Shards != nil }

func shardCount(c *kividbv1alpha1.KividbCluster) int32 {
	if c.Spec.Shards == nil {
		return 0
	}
	return *c.Spec.Shards
}

func shardStatefulSetName(c *kividbv1alpha1.KividbCluster, shard int32) string {
	return fmt.Sprintf("%s-%d", c.Name, shard)
}
func shardHeadlessServiceName(c *kividbv1alpha1.KividbCluster, shard int32) string {
	return shardStatefulSetName(c, shard) + "-headless"
}
func shardServiceName(c *kividbv1alpha1.KividbCluster, shard int32) string {
	return fmt.Sprintf("%s-shard-%d", c.Name, shard)
}
func shardPodName(c *kividbv1alpha1.KividbCluster, shard, n int32) string {
	return fmt.Sprintf("%s-%d", shardStatefulSetName(c, shard), n)
}
func clusterServiceName(c *kividbv1alpha1.KividbCluster) string { return c.Name }
func busSecretName(c *kividbv1alpha1.KividbCluster) string      { return c.Name + "-bus" }

func shardSelectorLabels(c *kividbv1alpha1.KividbCluster, shard int32) map[string]string {
	l := selectorLabels(c)
	l[ShardLabel] = strconv.Itoa(int(shard))
	return l
}

// validateSharded checks what is particular to a sharded spec.
func validateSharded(c *kividbv1alpha1.KividbCluster) error {
	if !isSharded(c) {
		if c.Spec.Cluster != nil {
			return fmt.Errorf("spec.cluster is for sharded clusters: set spec.shards too (only possible when the cluster is created)")
		}
		return nil
	}
	shards := shardCount(c)
	if shards < 1 {
		return fmt.Errorf("spec.shards must be at least 1, got %d", shards)
	}
	if nodes := shards * (c.Spec.Replicas + 1); nodes > maxShardedNodes {
		return fmt.Errorf("spec.shards × (spec.replicas + 1) = %d nodes; at most %d are supported for now", nodes, maxShardedNodes)
	}
	if len(c.Name) > maxShardedNameLen {
		return fmt.Errorf("a sharded cluster's name may have at most %d characters (%q has %d), so the names of its pods and Services stay valid", maxShardedNameLen, c.Name, len(c.Name))
	}
	if c.Spec.BootstrapFromSnapshot != nil {
		return fmt.Errorf("spec.bootstrapFromSnapshot is not supported for sharded clusters yet")
	}
	return nil
}

// operatorOwnedClusterDirective reports whether a KividbConfig directive is
// one the operator sets itself on a sharded cluster.
func operatorOwnedClusterDirective(key string) bool {
	k := strings.ToLower(strings.TrimSpace(key))
	if strings.HasPrefix(k, "cluster-") || strings.HasPrefix(k, "min-replicas-") || strings.HasPrefix(k, "min-slaves-") {
		return true
	}
	switch k {
	case "replicaof", "slaveof", "masterauth", "primaryauth", "masteruser", "primaryuser", "tls-cluster", "tls-replication":
		return true
	}
	return false
}

// clusterDirectives are the cluster-mode lines of a sharded cluster's
// kividb.conf. Per-pod values are not here: cluster-announce-ip is a
// command-line flag from the pod's own IP (see shardPodTemplate).
func clusterDirectives(c *kividbv1alpha1.KividbCluster, secrets clusterSecrets) map[string]string {
	spec := c.Spec.Cluster
	if spec == nil {
		spec = &kividbv1alpha1.ClusterModeSpec{}
	}
	timeout := int32(defaultNodeTimeoutMs)
	if spec.NodeTimeoutMs != nil {
		timeout = *spec.NodeTimeoutMs
	}
	coverage := "yes"
	if spec.RequireFullCoverage != nil && !*spec.RequireFullCoverage {
		coverage = "no"
	}
	minReplicas := int32(0)
	if spec.MinReplicasToWrite != nil {
		minReplicas = *spec.MinReplicasToWrite
	}
	maxLag := int32(defaultMinReplicasLag)
	if spec.MinReplicasMaxLagSeconds != nil {
		maxLag = *spec.MinReplicasMaxLagSeconds
	}

	d := map[string]string{
		"cluster-enabled":               "yes",
		"cluster-config-file":           "nodes.conf", // in the working directory, the data volume
		"cluster-node-timeout":          strconv.Itoa(int(timeout)),
		"cluster-require-full-coverage": coverage,
		// Nodes announce a name, not an IP, to clients: the pod's DNS name,
		// or the address of a Service the user exposed it through. The
		// agent sets it at runtime (cluster-announce-hostname).
		"cluster-preferred-endpoint-type": "hostname",
		"min-replicas-to-write":           strconv.Itoa(int(minReplicas)),
		"min-replicas-max-lag":            strconv.Itoa(int(maxLag)),
	}
	if secrets.busSecret != "" {
		d["cluster-bus-secret"] = secrets.busSecret
	}
	if secrets.masterAuth != "" {
		d["masteruser"] = "default"
		d["masterauth"] = secrets.masterAuth
	}
	return d
}

// ---------- workloads ----------

// shardAntiAffinity spreads a shard's pods across nodes first (its master
// and replicas failing together would take its slots down), then the
// cluster's. Preferred, not required, so small clusters still schedule.
func shardAntiAffinity(c *kividbv1alpha1.KividbCluster, shard int32) *corev1.Affinity {
	term := func(weight int32, labels map[string]string) corev1.WeightedPodAffinityTerm {
		return corev1.WeightedPodAffinityTerm{
			Weight: weight,
			PodAffinityTerm: corev1.PodAffinityTerm{
				LabelSelector: &metav1.LabelSelector{MatchLabels: labels},
				TopologyKey:   "kubernetes.io/hostname",
			},
		}
	}
	return &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
				term(100, shardSelectorLabels(c, shard)),
				term(50, selectorLabels(c)),
			},
		},
	}
}

func shardPodTemplate(c *kividbv1alpha1.KividbCluster, shard int32, kdbConfig *kividbv1alpha1.KividbConfig, aclConfig *kividbv1alpha1.KividbAclConfig, snapCfg *kividbv1alpha1.KividbSnapshotConfig) corev1.PodTemplateSpec {
	t := podTemplate(c, kdbConfig, aclConfig, snapCfg)
	t.Labels[ShardLabel] = strconv.Itoa(int(shard))
	if c.Spec.Affinity == nil {
		t.Spec.Affinity = shardAntiAffinity(c, shard)
	}
	for i := range t.Spec.Containers {
		if t.Spec.Containers[i].Name != "kividb" {
			continue
		}
		k := &t.Spec.Containers[i]
		// Peers reach the node at its pod IP. Fixed at startup; Kubernetes
		// expands $(POD_IP) from the container's environment.
		k.Env = append(k.Env, corev1.EnvVar{
			Name:      "POD_IP",
			ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"}},
		})
		k.Args = append(k.Args, "--cluster-announce-ip", "$(POD_IP)")
	}
	return t
}

func desiredShardStatefulSet(c *kividbv1alpha1.KividbCluster, shard int32, kdbConfig *kividbv1alpha1.KividbConfig, aclConfig *kividbv1alpha1.KividbAclConfig, snapCfg *kividbv1alpha1.KividbSnapshotConfig) *appsv1.StatefulSet {
	sts := desiredStatefulSet(c, kdbConfig, aclConfig, snapCfg)
	replicas := c.Spec.Replicas + 1
	labels := commonLabels(c)
	labels[ShardLabel] = strconv.Itoa(int(shard))
	sts.Name = shardStatefulSetName(c, shard)
	sts.Labels = labels
	sts.Spec.Replicas = &replicas
	sts.Spec.ServiceName = shardHeadlessServiceName(c, shard)
	sts.Spec.Selector = &metav1.LabelSelector{MatchLabels: shardSelectorLabels(c, shard)}
	sts.Spec.Template = shardPodTemplate(c, shard, kdbConfig, aclConfig, snapCfg)
	return sts
}

// desiredShardPDB keeps a node drain from taking more than one of a
// shard's pods at a time.
func desiredShardPDB(c *kividbv1alpha1.KividbCluster, shard int32) *policyv1.PodDisruptionBudget {
	one := intstr.FromInt32(1)
	labels := commonLabels(c)
	labels[ShardLabel] = strconv.Itoa(int(shard))
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: shardStatefulSetName(c, shard), Namespace: c.Namespace, Labels: labels},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: &one,
			Selector:       &metav1.LabelSelector{MatchLabels: shardSelectorLabels(c, shard)},
		},
	}
}

// ---------- Services: ClusterIP only ----------

func clientPorts(c *kividbv1alpha1.KividbCluster, kdbConfig *kividbv1alpha1.KividbConfig) []corev1.ServicePort {
	ports := []corev1.ServicePort{{Name: KividbPortName, Port: getPort(c), TargetPort: intstr.FromInt32(getPort(c))}}
	if tls := tlsServicePort(kdbConfig); tls != nil {
		ports = append(ports, *tls)
	}
	return ports
}

func clusterIPService(c *kividbv1alpha1.KividbCluster, name, kind string, shard *int32, selector map[string]string, ports []corev1.ServicePort) *corev1.Service {
	labels := commonLabels(c)
	labels[ServiceKindLabel] = kind
	if shard != nil {
		labels[ShardLabel] = strconv.Itoa(int(*shard))
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: c.Namespace, Labels: labels},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: selector,
			Ports:    ports,
		},
	}
}

// desiredShardedServices are a sharded cluster's Services: a headless one
// per shard (the StatefulSet's, for each pod's DNS name, and the only one
// with the agent port), one per pod, one per shard, one for the replicas
// and one for the whole cluster. All ClusterIP: the operator never
// provisions load balancers; users expose pods with Services of their own.
func desiredShardedServices(c *kividbv1alpha1.KividbCluster, kdbConfig *kividbv1alpha1.KividbConfig) []*corev1.Service {
	ports := clientPorts(c, kdbConfig)
	var out []*corev1.Service
	for s := int32(0); s < shardCount(c); s++ {
		shard := s
		headless := desiredHeadlessService(c, kdbConfig)
		headless.Name = shardHeadlessServiceName(c, shard)
		headless.Labels[ShardLabel] = strconv.Itoa(int(shard))
		headless.Spec.Selector = shardSelectorLabels(c, shard)
		out = append(out, headless)

		out = append(out, clusterIPService(c, shardServiceName(c, shard), "shard", &shard, shardSelectorLabels(c, shard), ports))
		for n := int32(0); n <= c.Spec.Replicas; n++ {
			pod := shardPodName(c, shard, n)
			selector := selectorLabels(c)
			selector[podNameLabel] = pod
			out = append(out, clusterIPService(c, pod, "pod", &shard, selector, ports))
		}
	}
	replicaSelector := selectorLabels(c)
	replicaSelector[kividbv1alpha1.RoleLabel] = string(kividbv1alpha1.RoleReplica)
	out = append(out, clusterIPService(c, replicaServiceName(c), "replicas", nil, replicaSelector, ports))
	out = append(out, clusterIPService(c, clusterServiceName(c), "cluster", nil, selectorLabels(c), ports))
	return out
}

func (r *KividbClusterReconciler) applyService(ctx context.Context, c *kividbv1alpha1.KividbCluster, desired *corev1.Service) error {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: c.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = desired.Labels
		svc.Spec.Type = desired.Spec.Type
		svc.Spec.Selector = desired.Spec.Selector
		svc.Spec.Ports = desired.Spec.Ports
		svc.Spec.PublishNotReadyAddresses = desired.Spec.PublishNotReadyAddresses
		if desired.Spec.ClusterIP == corev1.ClusterIPNone {
			svc.Spec.ClusterIP = corev1.ClusterIPNone
		}
		return controllerutil.SetControllerReference(c, svc, r.scheme())
	})
	if err != nil {
		return fmt.Errorf("service %s: %w", desired.Name, err)
	}
	return nil
}

func (r *KividbClusterReconciler) reconcileShardedServices(ctx context.Context, c *kividbv1alpha1.KividbCluster, kdbConfig *kividbv1alpha1.KividbConfig) error {
	want := map[string]bool{}
	for _, s := range desiredShardedServices(c, kdbConfig) {
		want[s.Name] = true
		if err := r.applyService(ctx, c, s); err != nil {
			return err
		}
	}
	// Services of pods and shards that are no longer wanted (replicas or
	// shards removed). Only the operator's own: a user's Service with the
	// same selector is never touched.
	var list corev1.ServiceList
	if err := r.List(ctx, &list, client.InNamespace(c.Namespace), client.MatchingLabels(selectorLabels(c))); err != nil {
		return err
	}
	for i := range list.Items {
		s := &list.Items[i]
		if want[s.Name] || !metav1.IsControlledBy(s, c) {
			continue
		}
		if err := r.Delete(ctx, s); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

// ---------- bus secret ----------

// reconcileBusSecret returns the cluster-bus-secret: from
// spec.cluster.busSecretRef, or generated once into <cluster>-bus.
func (r *KividbClusterReconciler) reconcileBusSecret(ctx context.Context, c *kividbv1alpha1.KividbCluster) (string, error) {
	if c.Spec.Cluster != nil && c.Spec.Cluster.BusSecretRef != nil {
		ref := c.Spec.Cluster.BusSecretRef
		var s corev1.Secret
		if err := r.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: ref.Name}, &s); err != nil {
			return "", fmt.Errorf("spec.cluster.busSecretRef: %w", err)
		}
		v := string(s.Data[ref.Key])
		if v == "" {
			return "", fmt.Errorf("spec.cluster.busSecretRef: Secret %s has no key %q", ref.Name, ref.Key)
		}
		return v, nil
	}
	s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: busSecretName(c), Namespace: c.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, s, func() error {
		if len(s.Data[busSecretKey]) == 0 {
			b := make([]byte, 32)
			if _, err := rand.Read(b); err != nil {
				return err
			}
			s.Data = map[string][]byte{busSecretKey: []byte(hex.EncodeToString(b))}
		}
		s.Labels = commonLabels(c)
		s.Type = corev1.SecretTypeOpaque
		return controllerutil.SetControllerReference(c, s, r.scheme())
	})
	if err != nil {
		return "", fmt.Errorf("bus secret: %w", err)
	}
	return string(s.Data[busSecretKey]), nil
}

// ---------- reconcile ----------

// reconcileSharded is the reconcile of a sharded cluster, after the parts
// both modes share (validation, references, the auth Secret).
//
// It creates the shards' workloads and Services, forms the cluster and
// looks after it (see cluster_form.go). Rollouts and backups of sharded
// clusters come in the next steps of 0.7.0.
func (r *KividbClusterReconciler) reconcileSharded(ctx context.Context, c *kividbv1alpha1.KividbCluster, kdbConfig *kividbv1alpha1.KividbConfig, aclConfig *kividbv1alpha1.KividbAclConfig, snapCfg *kividbv1alpha1.KividbSnapshotConfig, aclContent, defaultPassword string, generations authGenerations) (ctrl.Result, error) {
	bus, err := r.reconcileBusSecret(ctx, c)
	if err != nil {
		return ctrl.Result{}, err
	}
	secrets := clusterSecrets{busSecret: bus, masterAuth: defaultPassword}
	if err := r.reconcileConfig(ctx, c, kdbConfig, secrets); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling config: %w", err)
	}
	if err := r.reconcileShardedServices(ctx, c, kdbConfig); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconciling services: %w", err)
	}

	rollout := map[string]string{
		ConfigHashAnnotation:     configHash(renderKividbConfWith(c, kdbConfig, secrets))[:16],
		AuthGenerationAnnotation: generations.auth,
	}
	for s := int32(0); s < shardCount(c); s++ {
		if err := r.applyStatefulSet(ctx, c, desiredShardStatefulSet(c, s, kdbConfig, aclConfig, snapCfg), rollout); err != nil {
			return ctrl.Result{}, fmt.Errorf("reconciling statefulset of shard %d: %w", s, err)
		}
		if err := r.applyPDB(ctx, c, desiredShardPDB(c, s)); err != nil {
			return ctrl.Result{}, fmt.Errorf("reconciling disruption budget of shard %d: %w", s, err)
		}
	}

	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(c.Namespace), client.MatchingLabels(selectorLabels(c))); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing pods: %w", err)
	}
	r.reconcileAclReload(ctx, c, pods.Items, aclContent, generations)

	exposed, err := r.exposures(ctx, c)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("listing announced Services: %w", err)
	}
	observed := r.observeShardedPods(ctx, c, pods.Items)
	acted, topoErr := r.reconcileClusterTopology(ctx, c, observed, exposed)
	if topoErr != nil {
		logf.FromContext(ctx).Error(topoErr, "cluster topology")
	}
	r.labelShardedRoles(ctx, observed)

	if cond := exposureCondition(c, exposed); cond != nil {
		setCondition(&c.Status.Conditions, *cond)
	} else {
		removeCondition(&c.Status.Conditions, "ExternallyAccessible")
	}
	phase, err := r.updateShardedStatus(ctx, c, observed, topoErr)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("updating status: %w", err)
	}
	if acted || topoErr != nil || phase != kividbv1alpha1.PhaseRunning {
		return ctrl.Result{RequeueAfter: shardedFormingInterval}, nil
	}
	return ctrl.Result{RequeueAfter: reconcileInterval}, nil
}

// shardedFormingInterval is how soon a sharded cluster that is forming, or
// not healthy, is looked at again.
const shardedFormingInterval = 3 * time.Second

func (r *KividbClusterReconciler) applyPDB(ctx context.Context, c *kividbv1alpha1.KividbCluster, desired *policyv1.PodDisruptionBudget) error {
	pdb := &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: c.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
		pdb.Labels = desired.Labels
		pdb.Spec.MaxUnavailable = desired.Spec.MaxUnavailable
		pdb.Spec.Selector = desired.Spec.Selector
		return controllerutil.SetControllerReference(c, pdb, r.scheme())
	})
	return err
}

func (r *KividbClusterReconciler) updateShardedStatus(ctx context.Context, c *kividbv1alpha1.KividbCluster, pods []*shardPod, topoErr error) (kividbv1alpha1.ClusterPhase, error) {
	before := append([]kividbv1alpha1.ShardStatus(nil), c.Status.Shards...)
	phase, reason, message := shardedStatus(c, pods)
	r.noteShardFailovers(c, before)
	c.Status.Phase = phase
	c.Status.ObservedGeneration = c.Generation

	cond := metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: c.Generation,
	}
	if phase == kividbv1alpha1.PhaseRunning {
		cond.Status = metav1.ConditionTrue
	}
	if topoErr != nil && phase != kividbv1alpha1.PhaseRunning {
		cond.Reason = "ReconcileError"
		cond.Message = topoErr.Error()
	}
	setCondition(&c.Status.Conditions, cond)
	return phase, r.Status().Update(ctx, c)
}
