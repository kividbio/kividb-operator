package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func int32p(n int32) *int32 { return &n }

func shardedCluster(name string, shards, replicas int32) *kividbv1alpha1.KividbCluster {
	c := testCluster(name)
	c.Spec.Shards = int32p(shards)
	c.Spec.Replicas = replicas
	c.Spec.Storage = kividbv1alpha1.StorageSpec{Size: "1Gi"}
	return c
}

func TestValidateSharded(t *testing.T) {
	ok := []*kividbv1alpha1.KividbCluster{
		testCluster("plain"),
		shardedCluster("s", 3, 1),
		shardedCluster("s", 8, 2), // 24 nodes
	}
	for _, c := range ok {
		if err := validateSharded(c); err != nil {
			t.Errorf("%s: %v", c.Name, err)
		}
	}

	tooMany := shardedCluster("big", 9, 2) // 27 nodes
	longName := shardedCluster(strings.Repeat("a", maxShardedNameLen+1), 1, 0)
	zero := shardedCluster("zero", 0, 1)
	bootstrap := shardedCluster("boot", 2, 1)
	bootstrap.Spec.BootstrapFromSnapshot = &kividbv1alpha1.BootstrapFromSnapshotSpec{}
	clusterWithoutShards := testCluster("plain")
	clusterWithoutShards.Spec.Cluster = &kividbv1alpha1.ClusterModeSpec{}
	for _, c := range []*kividbv1alpha1.KividbCluster{tooMany, longName, zero, bootstrap, clusterWithoutShards} {
		if validateSharded(c) == nil {
			t.Errorf("%s: accepted", c.Name)
		}
	}
}

func TestShardedKividbConfigRefusesOperatorDirectives(t *testing.T) {
	c := shardedCluster("s", 3, 1)
	cfg := func(d map[string]string) *kividbv1alpha1.KividbConfig {
		return &kividbv1alpha1.KividbConfig{ObjectMeta: metav1.ObjectMeta{Name: "cfg"}, Spec: kividbv1alpha1.KividbConfigSpec{Directives: d}}
	}
	for _, key := range []string{"cluster-node-timeout", "Cluster-Enabled", "replicaof", "masterauth", "masteruser", "min-replicas-to-write", "min-slaves-max-lag", "tls-cluster"} {
		if validateKividbConfig(c, cfg(map[string]string{key: "x"})) == nil {
			t.Errorf("%s accepted in a sharded cluster", key)
		}
	}
	if validateKividbConfig(c, cfg(map[string]string{"cluster-replica-validity-factor": "0"})) == nil {
		t.Error("cluster-replica-validity-factor accepted: it is spec.cluster.replicaValidityFactor")
	}
	if err := validateKividbConfig(c, cfg(map[string]string{"maxmemory": "1gb", "appendonly": "yes"})); err != nil {
		t.Errorf("ordinary directives refused: %v", err)
	}
	// The slot migration settings are the user's (engine #111).
	if err := validateKividbConfig(c, cfg(map[string]string{
		"cluster-slot-migration-handoff-max-lag-bytes": "1mb",
		"Cluster-Slot-Migration-Write-Pause-Timeout":   "5000",
	})); err != nil {
		t.Errorf("slot migration settings refused: %v", err)
	}
}

func TestShardedConfigReplicaValidityFactor(t *testing.T) {
	c := shardedCluster("s", 3, 1)
	if s := renderKividbConfWith(c, nil, clusterSecrets{}); strings.Contains(s, "cluster-replica-validity-factor") {
		t.Errorf("cluster-replica-validity-factor rendered while unset (engines before 1.2 do not start with it):\n%s", s)
	}
	for _, v := range []int32{0, 10, 30} {
		c.Spec.Cluster = &kividbv1alpha1.ClusterModeSpec{ReplicaValidityFactor: int32p(v)}
		want := fmt.Sprintf("cluster-replica-validity-factor %d\n", v)
		if s := renderKividbConfWith(c, nil, clusterSecrets{}); !strings.Contains(s, want) {
			t.Errorf("missing %q in\n%s", want, s)
		}
	}
	c.Spec.Cluster = &kividbv1alpha1.ClusterModeSpec{ReplicaValidityFactor: int32p(5)}
	if cp := c.Spec.Cluster.DeepCopy(); cp.ReplicaValidityFactor == c.Spec.Cluster.ReplicaValidityFactor || *cp.ReplicaValidityFactor != 5 {
		t.Error("DeepCopy shares or loses replicaValidityFactor")
	}
}

func TestShardedConfig(t *testing.T) {
	c := shardedCluster("s", 3, 1)
	c.Spec.Cluster = &kividbv1alpha1.ClusterModeSpec{NodeTimeoutMs: int32p(8000), MinReplicasToWrite: int32p(1)}
	got := renderKividbConfWith(c, nil, clusterSecrets{busSecret: "bus-s3cret", masterAuth: "pw"})
	for _, want := range []string{
		"cluster-enabled yes\n", "cluster-config-file nodes.conf\n", "cluster-node-timeout 8000\n",
		"cluster-require-full-coverage yes\n", "cluster-preferred-endpoint-type hostname\n",
		"cluster-bus-secret bus-s3cret\n", "masteruser default\n", "masterauth pw\n",
		"min-replicas-to-write 1\n", "min-replicas-max-lag 10\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in\n%s", want, got)
		}
	}
	if strings.Contains(got, "replicaof") {
		t.Errorf("replicaof in a sharded config:\n%s", got)
	}
	// Without a password there is no masterauth; a replication cluster
	// gets no cluster lines at all.
	if s := renderKividbConfWith(c, nil, clusterSecrets{busSecret: "b"}); strings.Contains(s, "masterauth") {
		t.Errorf("masterauth without a password:\n%s", s)
	}
	if s := renderKividbConf(testCluster("plain"), nil); strings.Contains(s, "cluster-") {
		t.Errorf("cluster directives in a replication cluster:\n%s", s)
	}

	secret := desiredConfigSecret(c, nil, clusterSecrets{busSecret: "bus-s3cret"})
	if secret.Name != "s-config" || !strings.Contains(secret.StringData[ConfigFileName], "bus-s3cret") {
		t.Fatalf("config Secret: %s %v", secret.Name, secret.StringData)
	}
}

func TestShardStatefulSet(t *testing.T) {
	c := shardedCluster("s", 3, 2)
	sts := desiredShardStatefulSet(c, 1, nil, nil, nil)
	if sts.Name != "s-1" || *sts.Spec.Replicas != 3 || sts.Spec.ServiceName != "s-1-headless" {
		t.Fatalf("statefulset: %s replicas %d service %s", sts.Name, *sts.Spec.Replicas, sts.Spec.ServiceName)
	}
	if sts.Spec.Selector.MatchLabels[ShardLabel] != "1" || sts.Spec.Template.Labels[ShardLabel] != "1" {
		t.Fatalf("shard label missing: selector %v template %v", sts.Spec.Selector.MatchLabels, sts.Spec.Template.Labels)
	}
	var kividb *corev1.Container
	for i := range sts.Spec.Template.Spec.Containers {
		if sts.Spec.Template.Spec.Containers[i].Name == "kividb" {
			kividb = &sts.Spec.Template.Spec.Containers[i]
		}
	}
	if kividb == nil || !containsArgPair(kividb.Args, "--cluster-announce-ip", "$(POD_IP)") {
		t.Fatalf("kividb args: %v", kividb.Args)
	}
	hasPodIP := false
	for _, e := range kividb.Env {
		hasPodIP = hasPodIP || (e.Name == "POD_IP" && e.ValueFrom != nil && e.ValueFrom.FieldRef.FieldPath == "status.podIP")
	}
	if !hasPodIP {
		t.Fatal("no POD_IP in the kividb container's environment")
	}
	for _, v := range sts.Spec.Template.Spec.Volumes {
		if v.Name == "config" && (v.Secret == nil || v.Secret.SecretName != "s-config") {
			t.Fatalf("config volume: %+v", v)
		}
	}
	terms := sts.Spec.Template.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	if len(terms) != 2 || terms[0].PodAffinityTerm.LabelSelector.MatchLabels[ShardLabel] != "1" {
		t.Fatalf("anti-affinity: %+v", terms)
	}
}

func TestShardedServices(t *testing.T) {
	c := shardedCluster("s", 2, 1)
	svcs := desiredShardedServices(c, nil)
	var names []string
	byName := map[string]*corev1.Service{}
	for _, s := range svcs {
		names = append(names, s.Name)
		byName[s.Name] = s
	}
	sort.Strings(names)
	want := []string{"s", "s-0-0", "s-0-1", "s-0-headless", "s-1-0", "s-1-1", "s-1-headless", "s-replicas", "s-shard-0", "s-shard-1"}
	if strings.Join(names, " ") != strings.Join(want, " ") {
		t.Fatalf("services %v, want %v", names, want)
	}
	for _, s := range svcs {
		headless := strings.HasSuffix(s.Name, "-headless")
		hasAgent := false
		for _, p := range s.Spec.Ports {
			hasAgent = hasAgent || p.Name == AgentPortName
		}
		if headless != hasAgent {
			t.Errorf("%s: agent port %v", s.Name, hasAgent)
		}
		if !headless && s.Spec.Type != corev1.ServiceTypeClusterIP {
			t.Errorf("%s: type %s, want ClusterIP", s.Name, s.Spec.Type)
		}
		if headless && (s.Spec.ClusterIP != corev1.ClusterIPNone || s.Labels[ServiceKindLabel] != "headless") {
			t.Errorf("%s: not headless (%s, %v)", s.Name, s.Spec.ClusterIP, s.Labels)
		}
	}
	if byName["s-1-0"].Spec.Selector[podNameLabel] != "s-1-0" {
		t.Errorf("pod Service selector: %v", byName["s-1-0"].Spec.Selector)
	}
	if byName["s-shard-1"].Spec.Selector[ShardLabel] != "1" || byName["s-0-headless"].Spec.Selector[ShardLabel] != "0" {
		t.Error("shard selectors")
	}
	if byName["s-replicas"].Spec.Selector[kividbv1alpha1.RoleLabel] != "replica" {
		t.Errorf("replicas Service selector: %v", byName["s-replicas"].Spec.Selector)
	}

	pdb := desiredShardPDB(c, 1)
	if pdb.Name != "s-1" || pdb.Spec.MaxUnavailable.IntValue() != 1 || pdb.Spec.Selector.MatchLabels[ShardLabel] != "1" {
		t.Fatalf("pdb: %+v", pdb.Spec)
	}
}

func shardedReconciler(t *testing.T, objs ...client.Object) *KividbClusterReconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := kividbv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return &KividbClusterReconciler{
		Client:   fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).WithStatusSubresource(&kividbv1alpha1.KividbCluster{}).Build(),
		Scheme:   scheme,
		Agent:    NewAgentClient(0, "tok"),
		Recorder: record.NewFakeRecorder(100),
	}
}

func TestReconcileShardedCreatesTheShards(t *testing.T) {
	c := shardedCluster("s", 3, 1)
	r := shardedReconciler(t, c)
	ctx := context.Background()
	if _, err := r.reconcileSharded(ctx, c, nil, nil, nil, "", "", authGenerations{auth: "1"}); err != nil {
		t.Fatal(err)
	}

	var stsList appsv1.StatefulSetList
	_ = r.List(ctx, &stsList, client.InNamespace("default"))
	var pdbs policyv1.PodDisruptionBudgetList
	_ = r.List(ctx, &pdbs, client.InNamespace("default"))
	var svcs corev1.ServiceList
	_ = r.List(ctx, &svcs, client.InNamespace("default"))
	if len(stsList.Items) != 3 || len(pdbs.Items) != 3 || len(svcs.Items) != 3*(1+1+2)+2 {
		t.Fatalf("%d statefulsets, %d budgets, %d services", len(stsList.Items), len(pdbs.Items), len(svcs.Items))
	}

	var bus corev1.Secret
	if err := r.Get(ctx, client.ObjectKey{Namespace: "default", Name: "s-bus"}, &bus); err != nil || len(bus.Data[busSecretKey]) != 64 {
		t.Fatalf("bus secret: %v %v", bus.Data, err)
	}
	var cfg corev1.Secret
	if err := r.Get(ctx, client.ObjectKey{Namespace: "default", Name: "s-config"}, &cfg); err != nil {
		t.Fatal(err)
	}
	conf := string(cfg.Data[ConfigFileName]) + cfg.StringData[ConfigFileName]
	if !strings.Contains(conf, "cluster-bus-secret "+string(bus.Data[busSecretKey])) {
		t.Fatalf("config does not carry the bus secret:\n%s", conf)
	}

	var got kividbv1alpha1.KividbCluster
	_ = r.Get(ctx, client.ObjectKeyFromObject(c), &got)
	if got.Status.Phase != kividbv1alpha1.PhaseProvisioning || len(got.Status.Shards) != 3 {
		t.Fatalf("status: %s, %d shards", got.Status.Phase, len(got.Status.Shards))
	}

	// Again: the bus secret is kept, not regenerated.
	firstBus := string(bus.Data[busSecretKey])
	if _, err := r.reconcileSharded(ctx, &got, nil, nil, nil, "", "", authGenerations{auth: "1"}); err != nil {
		t.Fatal(err)
	}
	_ = r.Get(ctx, client.ObjectKey{Namespace: "default", Name: "s-bus"}, &bus)
	if string(bus.Data[busSecretKey]) != firstBus {
		t.Fatal("the bus secret changed on a second reconcile")
	}

	// Fewer replicas: the Services of the pods that go away are removed,
	// and a Service the user made with a pod's selector is left alone.
	user := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "my-lb-for-s-0-1", Namespace: "default", Labels: selectorLabels(c)},
		Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer, Selector: map[string]string{podNameLabel: "s-0-1"}},
	}
	if err := r.Create(ctx, user); err != nil {
		t.Fatal(err)
	}
	_ = r.Get(ctx, client.ObjectKeyFromObject(c), &got)
	got.Spec.Replicas = 0
	if _, err := r.reconcileSharded(ctx, &got, nil, nil, nil, "", "", authGenerations{auth: "1"}); err != nil {
		t.Fatal(err)
	}
	var after corev1.ServiceList
	_ = r.List(ctx, &after, client.InNamespace("default"))
	names := map[string]bool{}
	for _, s := range after.Items {
		names[s.Name] = true
	}
	if names["s-0-1"] || !names["s-0-0"] || !names["my-lb-for-s-0-1"] {
		t.Fatalf("services after scaling down: %v", names)
	}
}

func TestLegacyConfigMapIsRemovedOnceNoPodMountsIt(t *testing.T) {
	c := testCluster("demo")
	c.UID = "uid-demo"
	isController := true
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name: "demo-config", Namespace: "default",
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "kividb.io/v1alpha1", Kind: "KividbCluster", Name: "demo", UID: "uid-demo", Controller: &isController}},
	}}
	oldPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "default", Labels: selectorLabels(c)},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "config", VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "demo-config"}},
		}}}},
	}
	r := shardedReconciler(t, c, cm, oldPod)
	ctx := context.Background()

	if err := r.reconcileConfig(ctx, c, nil, clusterSecrets{}); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(cm), &corev1.ConfigMap{}); err != nil {
		t.Fatalf("removed while a pod still mounts it: %v", err)
	}

	_ = r.Delete(ctx, oldPod)
	if err := r.reconcileConfig(ctx, c, nil, clusterSecrets{}); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(cm), &corev1.ConfigMap{}); err == nil {
		t.Fatal("still there once no pod mounts it")
	}
	if err := r.Get(ctx, client.ObjectKey{Namespace: "default", Name: "demo-config"}, &corev1.Secret{}); err != nil {
		t.Fatalf("no config Secret: %v", err)
	}
}
