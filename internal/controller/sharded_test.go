package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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
	if _, err := r.reconcileSharded(ctx, c, nil, nil, nil, "", "", authGenerations{auth: "1"}, nil); err != nil {
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
	if _, err := r.reconcileSharded(ctx, &got, nil, nil, nil, "", "", authGenerations{auth: "1"}, nil); err != nil {
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
	if _, err := r.reconcileSharded(ctx, &got, nil, nil, nil, "", "", authGenerations{auth: "1"}, nil); err != nil {
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

// formedForReconcile is a formed, Running sharded cluster, stored so that
// Reconcile can read it, whose API server refuses the objects refuse
// names.
func formedForReconcile(t *testing.T, refuse func(client.Object) bool, objs ...client.Object) *formingHarness {
	t.Helper()
	h := newFormingHarness(t, 2, 1)
	for i := 0; i < 4; i++ {
		h.pass()
	}
	if h.c.Status.Phase != kividbv1alpha1.PhaseRunning {
		t.Fatalf("phase %s, want a formed cluster", h.c.Status.Phase)
	}
	ctx := context.Background()
	status := *h.c.Status.DeepCopy()
	for _, o := range append(objs, h.c) {
		if err := h.r.Create(ctx, o); err != nil {
			t.Fatal(err)
		}
	}
	h.c.Status = status
	h.c.Status.Conditions = []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Formed", LastTransitionTime: metav1.Now()}}
	if err := h.r.Status().Update(ctx, h.c); err != nil {
		t.Fatal(err)
	}
	refused := func(obj client.Object) error {
		if refuse != nil && refuse(obj) {
			return fmt.Errorf("%s is invalid", obj.GetName())
		}
		return nil
	}
	h.r.Client = interceptor.NewClient(h.r.Client.(client.WithWatch), interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if err := refused(obj); err != nil {
				return err
			}
			return cl.Create(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if err := refused(obj); err != nil {
				return err
			}
			return cl.Update(ctx, obj, opts...)
		},
	})
	// A role label the pass must set again: it still looks after the pods.
	var pod corev1.Pod
	_ = h.r.Get(ctx, client.ObjectKey{Namespace: "default", Name: "s-1-1"}, &pod)
	delete(pod.Labels, kividbv1alpha1.RoleLabel)
	if err := h.r.Update(ctx, &pod); err != nil {
		t.Fatal(err)
	}
	return h
}

// reconcileFormed runs Reconcile on h's cluster and checks the pods were
// still looked after and that Ready is False for reason, saying msg.
func reconcileFormed(t *testing.T, h *formingHarness, reason, msg string) {
	t.Helper()
	ctx := context.Background()
	res, err := h.r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(h.c)})
	if err != nil || res.RequeueAfter != shardedFormingInterval {
		t.Errorf("Reconcile = %+v, %v; want a requeue in %s and no error", res, err, shardedFormingInterval)
	}
	var got kividbv1alpha1.KividbCluster
	if err := h.r.Get(ctx, client.ObjectKeyFromObject(h.c), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != kividbv1alpha1.PhaseRunning || len(got.Status.Shards) != 2 {
		t.Errorf("status: phase %s, %d shards", got.Status.Phase, len(got.Status.Shards))
	}
	ready := meta.FindStatusCondition(got.Status.Conditions, "Ready")
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != reason || !strings.Contains(ready.Message, msg) {
		t.Errorf("Ready condition %+v, want False, %s, %q", ready, reason, msg)
	}
	var pod corev1.Pod
	_ = h.r.Get(ctx, client.ObjectKey{Namespace: "default", Name: "s-1-1"}, &pod)
	if pod.Labels[kividbv1alpha1.RoleLabel] != "replica" {
		t.Errorf("s-1-1 role label %q, want replica set again", pod.Labels[kividbv1alpha1.RoleLabel])
	}
}

// A step of a sharded cluster the API server refuses does not stop the
// pods from being looked after, and the cluster is not Ready, though all
// its pods are.
func TestReconcileSharded_RefusedApplyStepIsNotReady(t *testing.T) {
	snap := &kividbv1alpha1.KividbSnapshotConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "hourly", Namespace: "default"},
		Spec: kividbv1alpha1.KividbSnapshotConfigSpec{Schedule: "0 * * * *",
			S3: kividbv1alpha1.S3StorageSpec{Endpoint: "https://s3.example.com", Bucket: "b"}},
	}
	h := formedForReconcile(t, func(obj client.Object) bool {
		_, ok := obj.(*batchv1.CronJob)
		return ok
	}, snap)
	h.c.Spec.SnapshotConfigRef = &corev1.LocalObjectReference{Name: "hourly"}
	if err := h.r.Update(context.Background(), h.c); err != nil {
		t.Fatal(err)
	}
	reconcileFormed(t, h, "ReconcileError", "reconciling backup cronjob: s-backup is invalid")
}

// A sharded cluster whose referenced object cannot be read has nothing
// written, but its pods are still looked after, and it is not Ready.
func TestReconcileSharded_UnreadableReferenceIsNotReady(t *testing.T) {
	h := formedForReconcile(t, nil)
	h.c.Spec.ConfigRef = &corev1.LocalObjectReference{Name: "conf"}
	if err := h.r.Update(context.Background(), h.c); err != nil {
		t.Fatal(err)
	}
	reconcileFormed(t, h, "ReconcileError", `resolving configRef: KividbConfig "conf"`)
	var list appsv1.StatefulSetList
	_ = h.r.List(context.Background(), &list, client.InNamespace("default"))
	if len(list.Items) != 0 {
		t.Errorf("%d StatefulSets written, want none", len(list.Items))
	}
}

// Held back with spec.port changed, a sharded cluster's pods still listen
// on the old port: the topology, which introduces nodes at spec.port, is
// left alone; with the port unchanged it still runs.
func TestReconcileSharded_HeldBackPortChangeLeavesTopologyAlone(t *testing.T) {
	for _, tc := range []struct {
		name     string
		port     int32
		wantMeet bool
	}{{"port unchanged", 0, true}, {"port changed", 6390, false}} {
		t.Run(tc.name, func(t *testing.T) {
			h := formedForReconcile(t, nil)
			ctx := context.Background()
			for _, p := range h.pods() {
				p.Spec.Containers = []corev1.Container{{Name: kividbContainerName,
					Ports: []corev1.ContainerPort{{Name: KividbPortName, ContainerPort: 6380}}}}
				if err := h.r.Update(ctx, &p); err != nil {
					t.Fatal(err)
				}
				// s-1-1 came back with a new IP: the others must meet it again.
				if p.Name == "s-1-1" {
					n := h.node(p.Name)
					delete(h.engine.byIP, n.ip)
					n.ip, p.Status.PodIP = "10.0.9.9", "10.0.9.9"
					h.engine.byIP[n.ip] = n.id
					if err := h.r.Status().Update(ctx, &p); err != nil {
						t.Fatal(err)
					}
				}
			}
			_ = h.r.Get(ctx, client.ObjectKeyFromObject(h.c), h.c)
			h.c.Spec.ConfigRef = &corev1.LocalObjectReference{Name: "conf"}
			h.c.Spec.Port = tc.port
			if err := h.r.Update(ctx, h.c); err != nil {
				t.Fatal(err)
			}
			h.engine.calls = nil
			if _, err := h.r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(h.c)}); err != nil {
				t.Fatal(err)
			}
			met := false
			for _, call := range h.engine.calls {
				met = met || strings.HasPrefix(call, "/cluster/meet")
			}
			if met != tc.wantMeet {
				t.Errorf("met: %v, want %v (engine calls %v)", met, tc.wantMeet, h.engine.calls)
			}
		})
	}
}

// A shard added while a step is refused: moving slots waits only for the
// shards' StatefulSets and budgets, not for a refused backup CronJob.
func TestReconcileSharded_RebalanceWaitsOnlyForTheWorkloads(t *testing.T) {
	snap := &kividbv1alpha1.KividbSnapshotConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "hourly", Namespace: "default"},
		Spec: kividbv1alpha1.KividbSnapshotConfigSpec{Schedule: "0 * * * *",
			S3: kividbv1alpha1.S3StorageSpec{Endpoint: "https://s3.example.com", Bucket: "b"}},
	}
	for _, tc := range []struct {
		name      string
		refuse    func(client.Object) bool
		wantMoves bool
	}{
		{"nothing refused", nil, true},
		{"backup CronJob refused", func(obj client.Object) bool {
			_, ok := obj.(*batchv1.CronJob)
			return ok
		}, true},
		{"StatefulSets refused", func(obj client.Object) bool {
			_, ok := obj.(*appsv1.StatefulSet)
			return ok
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := formedForReconcile(t, tc.refuse, snap.DeepCopy())
			ctx := context.Background()
			// The new shard's pods, as its StatefulSet made them on an
			// earlier pass.
			for n := int32(0); n <= h.c.Spec.Replicas; n++ {
				ip := fmt.Sprintf("10.0.2.%d", n+1)
				p := shardedPod(h.c, 2, n, ip)
				if err := h.r.Create(ctx, p); err != nil {
					t.Fatal(err)
				}
				h.engine.add(nodeID(p.Name), ip)
			}
			_ = h.r.Get(ctx, client.ObjectKeyFromObject(h.c), h.c)
			h.c.Spec.Shards = int32p(3)
			h.c.Spec.SnapshotConfigRef = &corev1.LocalObjectReference{Name: "hourly"}
			if err := h.r.Update(ctx, h.c); err != nil {
				t.Fatal(err)
			}
			h.engine.calls = nil
			for i := 0; i < 8; i++ {
				if _, err := h.r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(h.c)}); err != nil {
					t.Fatal(err)
				}
			}
			moved := false
			for _, call := range h.engine.calls {
				moved = moved || strings.HasPrefix(call, "/cluster/migration/import")
			}
			if moved != tc.wantMoves {
				t.Errorf("slots moved: %v, want %v (%d slots; engine calls %v)", moved, tc.wantMoves, h.engine.moved, h.engine.calls)
			}
		})
	}
}

// A pass that read the cluster from a cache a version behind, before the
// pass that finished a 3 -> 2 wrote its status, must not write its own
// over it: that brought the finished rebalance back.
func TestStaleStatusDoesNotUndoANewerOne(t *testing.T) {
	ctx := context.Background()
	c := shardedCluster("s", 2, 1)
	c.Status.Cluster = &kividbv1alpha1.ClusterModeStatus{Shards: 3, Rebalance: &kividbv1alpha1.RebalanceStatus{From: 3, To: 2, Phase: kividbv1alpha1.RebalanceMovingSlots}}
	r := shardedReconciler(t, c)
	var stale kividbv1alpha1.KividbCluster
	if err := r.Get(ctx, client.ObjectKeyFromObject(c), &stale); err != nil {
		t.Fatal(err)
	}
	read := stale.Status.DeepCopy()

	var finished kividbv1alpha1.KividbCluster
	_ = r.Get(ctx, client.ObjectKeyFromObject(c), &finished)
	finished.Status.Cluster.Shards, finished.Status.Cluster.Rebalance = 2, nil
	if err := r.Status().Update(ctx, &finished); err != nil {
		t.Fatal(err)
	}

	stale.Status.Phase = kividbv1alpha1.PhaseRebalancing
	if err := r.writeStatus(ctx, stale.DeepCopy(), read); !errors.Is(err, errStaleStatus) {
		t.Errorf("stale status written: %v", err)
	}
	// A whole pass from the stale object: no error, looked at again soon.
	res, err := r.reconcileSharded(ctx, &stale, nil, nil, nil, "", "", authGenerations{auth: "1"}, nil)
	if err != nil || res.RequeueAfter != shardedFormingInterval {
		t.Errorf("stale pass: %+v, %v", res, err)
	}
	var got kividbv1alpha1.KividbCluster
	_ = r.Get(ctx, client.ObjectKeyFromObject(c), &got)
	if rb := rebalanceOf(&got); rb != nil || got.Status.Cluster.Shards != 2 {
		t.Fatalf("the finished rebalance came back: %+v, %d shards", rb, got.Status.Cluster.Shards)
	}
}

// A conflict from a change outside the status (a label, spec.shards) is
// settled as before: the status still lands on the latest version.
func TestStatusLandsAfterAConflictOutsideIt(t *testing.T) {
	ctx := context.Background()
	c := shardedCluster("s", 3, 1)
	c.Status.Cluster = &kividbv1alpha1.ClusterModeStatus{Shards: 3}
	r := shardedReconciler(t, c)
	var stale kividbv1alpha1.KividbCluster
	if err := r.Get(ctx, client.ObjectKeyFromObject(c), &stale); err != nil {
		t.Fatal(err)
	}
	read := stale.Status.DeepCopy()

	var edited kividbv1alpha1.KividbCluster
	_ = r.Get(ctx, client.ObjectKeyFromObject(c), &edited)
	edited.Labels = map[string]string{"team": "a"}
	if err := r.Update(ctx, &edited); err != nil {
		t.Fatal(err)
	}

	stale.Status.Phase = kividbv1alpha1.PhaseRunning
	if err := r.writeStatus(ctx, &stale, read); err != nil {
		t.Fatal(err)
	}
	var got kividbv1alpha1.KividbCluster
	_ = r.Get(ctx, client.ObjectKeyFromObject(c), &got)
	if got.Status.Phase != kividbv1alpha1.PhaseRunning || got.Labels["team"] != "a" {
		t.Fatalf("phase %q, labels %v", got.Status.Phase, got.Labels)
	}
}
