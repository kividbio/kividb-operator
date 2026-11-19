package controller

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	"github.com/kividbio/kividb-operator/internal/agentapi"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// dirConfig is a KividbConfig with dir, nil for "".
func dirConfig(dir string) *kividbv1alpha1.KividbConfig {
	if dir == "" {
		return nil
	}
	return directivesConfig(map[string]string{"dir": dir})
}

// holdHarness is a reconciler over a fake client, whose agents answer
// CONFIG GET dir as agents says, for the tests of a held `dir` change
// (#112).
type holdHarness struct {
	t      *testing.T
	c      *kividbv1alpha1.KividbCluster
	r      *KividbClusterReconciler
	agents *fakeAgents
}

func newHoldHarness(t *testing.T, c *kividbv1alpha1.KividbCluster) *holdHarness {
	t.Helper()
	c.Spec.Storage = kividbv1alpha1.StorageSpec{Size: "1Gi"}
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := kividbv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	agents := &fakeAgents{status: map[string]*agentapi.StatusResponse{}, dirs: map[string]string{}, configRefused: map[string]bool{}}
	r := &KividbClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(c).
			WithStatusSubresource(&kividbv1alpha1.KividbCluster{}, &kividbv1alpha1.KividbDbOps{}).Build(),
		Scheme:   scheme,
		Agent:    &AgentClient{http: &http.Client{Transport: agents}},
		Recorder: record.NewFakeRecorder(100),
	}
	return &holdHarness{t: t, c: c, r: r, agents: agents}
}

// rollout is the rollout annotations of the pod template for cfg; hash,
// if set, is its config hash instead (as an older operator rendered it).
func (h *holdHarness) rollout(cfg *kividbv1alpha1.KividbConfig, hash string) map[string]string {
	if hash == "" {
		hash = configHash(renderKividbConf(h.c, cfg))[:16]
	}
	return map[string]string{ConfigHashAnnotation: hash, AuthGenerationAnnotation: "1"}
}

// apply applies the StatefulSets of cfg, as a reconcile pass does.
func (h *holdHarness) apply(cfg *kividbv1alpha1.KividbConfig, hash string) {
	h.t.Helper()
	ctx := context.Background()
	if !isSharded(h.c) {
		if err := h.r.applyStatefulSet(ctx, h.c, desiredStatefulSet(h.c, cfg, nil, nil), h.rollout(cfg, hash)); err != nil {
			h.t.Fatal(err)
		}
		return
	}
	for s := int32(0); s < shardCount(h.c); s++ {
		if err := h.r.applyStatefulSet(ctx, h.c, desiredShardStatefulSet(h.c, s, cfg, nil, nil), h.rollout(cfg, hash)); err != nil {
			h.t.Fatal(err)
		}
	}
}

func (h *holdHarness) sts(name string) *appsv1.StatefulSet {
	h.t.Helper()
	var sts appsv1.StatefulSet
	if err := h.r.Get(context.Background(), client.ObjectKey{Namespace: h.c.Namespace, Name: name}, &sts); err != nil {
		h.t.Fatal(err)
	}
	return &sts
}

// stsNames is the names of the cluster's StatefulSets.
func (h *holdHarness) stsNames() []string {
	if !isSharded(h.c) {
		return []string{statefulSetName(h.c)}
	}
	var names []string
	for s := int32(0); s < shardCount(h.c); s++ {
		names = append(names, shardStatefulSetName(h.c, s))
	}
	return names
}

// makePods makes every pod of every StatefulSet from its template, as the
// StatefulSet controller does, Ready, at an address whose agent answers.
func (h *holdHarness) makePods() []corev1.Pod {
	h.t.Helper()
	var pods []corev1.Pod
	n := 0
	for _, name := range h.stsNames() {
		sts := h.sts(name)
		for i := int32(0); i < *sts.Spec.Replicas; i++ {
			n++
			ip := fmt.Sprintf("10.0.0.%d", n)
			h.agents.status[ip] = &agentapi.StatusResponse{Role: agentapi.RoleMaster}
			pods = append(pods, *h.recreate(sts, fmt.Sprintf("%s-%d", name, i), ip))
		}
	}
	return pods
}

// recreate makes pod name from sts's template, as the StatefulSet
// controller does when a pod is gone: a drain, an eviction, `kubectl
// delete pod` or a KividbDbOps restart.
func (h *holdHarness) recreate(sts *appsv1.StatefulSet, name, ip string) *corev1.Pod {
	h.t.Helper()
	ctx := context.Background()
	old := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: h.c.Namespace}}
	_ = h.r.Delete(ctx, old)
	t := sts.Spec.Template.DeepCopy()
	labels := map[string]string{appsv1.StatefulSetRevisionLabel: "rev"}
	for k, v := range t.Labels {
		labels[k] = v
	}
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: h.c.Namespace, Labels: labels, Annotations: t.Annotations},
		Spec:       t.Spec,
		Status:     corev1.PodStatus{PodIP: ip, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}
	if err := h.r.Create(ctx, p); err != nil {
		h.t.Fatal(err)
	}
	return p
}

// writePodStatus writes pod's status, through the status subresource if
// the fake has one for pods.
func (h *holdHarness) writePodStatus(pod *corev1.Pod) {
	h.t.Helper()
	ctx := context.Background()
	if err := h.r.Status().Update(ctx, pod); err != nil {
		if err := h.r.Update(ctx, pod); err != nil {
			h.t.Fatal(err)
		}
	}
	var got corev1.Pod
	if err := h.r.Get(ctx, client.ObjectKeyFromObject(pod), &got); err != nil {
		h.t.Fatal(err)
	}
	if !apiequality.Semantic.DeepEqual(got.Status, pod.Status) {
		h.t.Fatalf("pod status not written: %+v", got.Status)
	}
}

// A `dir` change held on a cluster with no replica keeps the StatefulSet's
// pod template as it is (#112): a pod recreated while it is held (a drain,
// an eviction, `kubectl delete pod`) comes back on its own `dir`, not on
// an empty new one, and the rollout says why it holds. Set back, the
// template is applied again and the hold is gone.
func TestHeldDirChangeKeepsThePodTemplate(t *testing.T) {
	for _, tc := range []struct{ name, from, to string }{
		{"dir changed", "sub", "other"},
		{"dir removed", "sub", ""},
		{"dir added", "", "sub"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			c := testCluster("c1")
			h := newHoldHarness(t, c)
			h.apply(dirConfig(tc.from), "")
			before := h.sts("c1").Spec.Template.DeepCopy()
			pods := h.makePods()

			h.apply(dirConfig(tc.to), "")
			sts := h.sts("c1")
			if !apiequality.Semantic.DeepEqual(sts.Spec.Template, *before) {
				t.Fatalf("template changed while held:\n%+v\nwant\n%+v", sts.Spec.Template, *before)
			}
			if hold := sts.Annotations[DataDirHoldAnnotation]; !strings.Contains(hold, `"c1-0"`) {
				t.Fatalf("hold annotation %q, want c1-0 in it", hold)
			}

			// Recreated meanwhile, it is as it was: its engine takes the dir
			// it had, as one restarting in place does.
			recreated := h.recreate(sts, "c1-0", "10.0.0.1")
			if got, want := podDataDir(&recreated.Spec), engineDataDir(dirConfig(tc.from)); got != want {
				t.Fatalf("recreated pod's dir %s, want %s, the one it had", got, want)
			}

			h.r.reconcileRollout(ctx, c, []corev1.Pod{*recreated}, []kividbv1alpha1.KividbPodStatus{{Name: "c1-0", Role: kividbv1alpha1.RoleMaster, Ready: true, Synced: true}}, "c1-0")
			cond := findCondition(c.Status.Conditions, RolloutProgressingCondition)
			if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "WouldLoseData" ||
				!strings.Contains(cond.Message, "c1-0") || !strings.Contains(cond.Message, "keeps its current pod template") || strings.Contains(cond.Message, "delete") {
				t.Fatalf("RolloutProgressing %+v, want WouldLoseData, naming c1-0, saying the template is kept, and not offering to delete the pod", cond)
			}
			if _, ok := pods[0].Annotations[ConfigHashAnnotation]; !ok {
				t.Fatal("pod without a config hash")
			}

			h.apply(dirConfig(tc.from), "")
			sts = h.sts("c1")
			if _, held := sts.Annotations[DataDirHoldAnnotation]; held {
				t.Fatalf("dir set back: still held: %q", sts.Annotations[DataDirHoldAnnotation])
			}
			if got, want := podDataDir(&sts.Spec.Template.Spec), engineDataDir(dirConfig(tc.from)); got != want {
				t.Fatalf("dir set back: template dir %s, want %s", got, want)
			}
		})
	}
}

// With a replica, a `dir` change is not held: the replica is replaced
// first and resyncs, and the master hands over before its own turn.
func TestDirChangeWithAReplicaIsApplied(t *testing.T) {
	c := testCluster("c1")
	c.Spec.Replicas = 1
	h := newHoldHarness(t, c)
	h.apply(nil, "")
	h.makePods()
	h.apply(dirConfig("sub"), "")
	sts := h.sts("c1")
	if _, held := sts.Annotations[DataDirHoldAnnotation]; held || podDataDir(&sts.Spec.Template.Spec) != "/data/sub" {
		t.Fatalf("template dir %s, hold %q; want /data/sub applied, no hold", podDataDir(&sts.Spec.Template.Spec), sts.Annotations[DataDirHoldAnnotation])
	}
}

// A sharded cluster holds a `dir` change with replicas or not: every
// shard's StatefulSet keeps its template, and a pod recreated meanwhile is
// on its own dir, its nodes.conf with it (#112).
func TestHeldDirChangeKeepsTheShardTemplates(t *testing.T) {
	c := shardedCluster("s", 2, 1)
	h := newHoldHarness(t, c)
	h.apply(dirConfig("sub"), "")
	before := map[string]*corev1.PodTemplateSpec{}
	for _, name := range h.stsNames() {
		before[name] = h.sts(name).Spec.Template.DeepCopy()
	}
	h.makePods()
	h.apply(nil, "")
	for _, name := range h.stsNames() {
		sts := h.sts(name)
		if !apiequality.Semantic.DeepEqual(sts.Spec.Template, *before[name]) {
			t.Fatalf("%s: template changed while held", name)
		}
		if hold := sts.Annotations[DataDirHoldAnnotation]; !strings.Contains(hold, `"`+name+`-0"`) || !strings.Contains(hold, `"`+name+`-1"`) {
			t.Fatalf("%s: hold annotation %q, want both its pods", name, hold)
		}
		if p := h.recreate(sts, name+"-0", "10.0.9.1"); podDataDir(&p.Spec) != "/data/sub" {
			t.Fatalf("%s-0 recreated on %s, want /data/sub", name, podDataDir(&p.Spec))
		}
	}
	h.apply(dirConfig("sub"), "")
	for _, name := range h.stsNames() {
		if hold, held := h.sts(name).Annotations[DataDirHoldAnnotation]; held {
			t.Fatalf("%s: dir set back, still held: %q", name, hold)
		}
	}
}

// The sharded rollout reports a `dir` change held back from the
// StatefulSets, and replaces nothing.
func TestShardedRolloutReportsAHeldDirChange(t *testing.T) {
	h := formedForRollout(t, 2, 1)
	ctx := context.Background()
	for s := int32(0); s < shardCount(h.c); s++ {
		var sts appsv1.StatefulSet
		if err := h.r.Get(ctx, client.ObjectKey{Namespace: h.c.Namespace, Name: shardStatefulSetName(h.c, s)}, &sts); err != nil {
			t.Fatal(err)
		}
		pod := shardPodName(h.c, s, 0)
		sts.Annotations = map[string]string{DataDirHoldAnnotation: encodeDataDirHold([]dataDirMove{{Pod: pod, From: "/data/sub", To: "/data", Ready: true}})}
		if err := h.r.Update(ctx, &sts); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		if deleted := h.rolloutPass(); len(deleted) > 0 {
			t.Fatalf("pass %d replaced %v, want nothing while held", i, deleted)
		}
	}
	cond := findCondition(h.c.Status.Conditions, RolloutProgressingCondition)
	if cond == nil || cond.Reason != "WouldLoseData" || !strings.Contains(cond.Message, "s-0-0, s-1-0 from /data/sub to /data") ||
		!strings.Contains(cond.Message, "not supported on a running sharded cluster") || !strings.Contains(cond.Message, "keeps its current pod template") {
		t.Fatalf("condition %+v", cond)
	}
}

// A pod made by operator 0.7.1, whose engine runs in a `dir` made by hand
// (set through the KividbConfig, the directory created manually), has no
// data-dir init container: its spec does not say where its files are
// (#112). The engine does, and is asked.
func TestAPodWithoutTheInitContainerIsAskedWhereItsFilesAre(t *testing.T) {
	// The 0.7.1 template: no init container, no --dir, the engine takes
	// `dir sub` from its config file. Its config hash is 0.7.1's.
	setup := func(t *testing.T) (*holdHarness, *corev1.Pod) {
		h := newHoldHarness(t, testCluster("c1"))
		h.apply(nil, "0-7-1-hash")
		pods := h.makePods()
		if _, ok := specDataDir(&pods[0].Spec); ok {
			t.Fatal("want a pod whose spec does not say")
		}
		h.agents.dirs[pods[0].Status.PodIP] = "/data/sub"
		return h, &pods[0]
	}

	t.Run("dir set back: not rolled into an empty /data", func(t *testing.T) {
		h, pod := setup(t)
		before := h.sts("c1").Spec.Template.DeepCopy()
		h.apply(nil, "")
		sts := h.sts("c1")
		if !apiequality.Semantic.DeepEqual(sts.Spec.Template, *before) {
			t.Fatal("template applied: the pod would be rolled into /data, where its files are not")
		}
		moves, _ := heldDataDirMoves(sts)
		if len(moves) != 1 || moves[0].From != "/data/sub" || moves[0].To != "/data" {
			t.Fatalf("held %+v, want c1-0 from /data/sub to /data", moves)
		}
		h.r.reconcileRollout(context.Background(), h.c, []corev1.Pod{*pod}, nil, "c1-0")
		if cond := findCondition(h.c.Status.Conditions, RolloutProgressingCondition); cond == nil || !strings.Contains(cond.Message, "c1-0 from /data/sub to /data") {
			t.Fatalf("condition %+v, want it to say c1-0's files are in /data/sub", cond)
		}
	})

	t.Run("dir kept: the template takes it over", func(t *testing.T) {
		h, pod := setup(t)
		h.apply(dirConfig("sub"), "")
		sts := h.sts("c1")
		if _, held := sts.Annotations[DataDirHoldAnnotation]; held || podDataDir(&sts.Spec.Template.Spec) != "/data/sub" {
			t.Fatalf("template dir %s, hold %q; want /data/sub applied", podDataDir(&sts.Spec.Template.Spec), sts.Annotations[DataDirHoldAnnotation])
		}
		// And the rollout, replacing it, finds it where its replacement looks.
		if m, moved := h.r.dataDirMoved(context.Background(), pod, sts); moved {
			t.Fatalf("rollout sees a move %+v, want none", m)
		}
	})

	t.Run("dir unknown: CONFIG refused", func(t *testing.T) {
		h, pod := setup(t)
		h.agents.configRefused[pod.Status.PodIP] = true
		before := h.sts("c1").Spec.Template.DeepCopy()
		h.apply(nil, "")
		sts := h.sts("c1")
		if !apiequality.Semantic.DeepEqual(sts.Spec.Template, *before) {
			t.Fatal("template applied with the pod's dir unknown")
		}
		h.r.reconcileRollout(context.Background(), h.c, []corev1.Pod{*pod}, nil, "c1-0")
		cond := findCondition(h.c.Status.Conditions, RolloutProgressingCondition)
		for _, want := range []string{"c1-0 to /data, from where is not known (CONFIG GET dir: ", "NOPERM", "Where the files of c1-0 are is not known", "CONFIG GET dir as a user allowed CONFIG", "set dir to that"} {
			if cond == nil || !strings.Contains(cond.Message, want) {
				t.Fatalf("condition %+v, want %q in its message", cond, want)
			}
		}
		// Even with the KividbConfig's own dir: the engine might be anywhere.
		h.apply(dirConfig("sub"), "")
		if _, held := h.sts("c1").Annotations[DataDirHoldAnnotation]; !held {
			t.Fatal("dir sub with the pod's dir unknown: not held")
		}
	})

	t.Run("dir unknown: no pod IP", func(t *testing.T) {
		h, pod := setup(t)
		pod.Status.PodIP = ""
		h.writePodStatus(pod)
		h.apply(nil, "")
		moves, held := heldDataDirMoves(h.sts("c1"))
		if !held || len(moves) != 1 || moves[0].Why == "" {
			t.Fatalf("held %v %+v, want c1-0 held, its dir not known", held, moves)
		}
	})
}

// A cluster with no `dir` at all is not held for want of asking its
// engine: a pod made with the config it is to run with is in /data,
// without a question (the engine refuses CONFIG here, and is not asked).
func TestNoDirClusterIsNotHeldForAChangeElsewhere(t *testing.T) {
	h := newHoldHarness(t, testCluster("c1"))
	h.apply(nil, "")
	pods := h.makePods()
	h.agents.configRefused[pods[0].Status.PodIP] = true
	h.c.Spec.Image = "kividb:new"
	h.apply(nil, "")
	sts := h.sts("c1")
	if _, held := sts.Annotations[DataDirHoldAnnotation]; held {
		t.Fatalf("held: %q", sts.Annotations[DataDirHoldAnnotation])
	}
	if sts.Spec.Template.Spec.Containers[0].Image != "kividb:new" {
		t.Fatalf("image %s, want the new one applied", sts.Spec.Template.Spec.Containers[0].Image)
	}
}

// An engine that restarted in its pod read the config file as it was
// then: one that ran on a `dir` since set back is asked, and its word
// wins over the config hash its pod was made with (#112).
func TestARestartedEngineIsAskedWhereItsFilesAre(t *testing.T) {
	h := newHoldHarness(t, testCluster("c1"))
	h.apply(nil, "")
	pods := h.makePods()
	pod := &pods[0]
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: kividbContainerName, RestartCount: 1}}
	h.writePodStatus(pod)
	h.agents.dirs[pod.Status.PodIP] = "/data/old"
	h.c.Spec.Image = "kividb:new"
	h.apply(nil, "")
	moves, held := heldDataDirMoves(h.sts("c1"))
	if !held || len(moves) != 1 || moves[0].From != "/data/old" {
		t.Fatalf("held %v %+v, want c1-0 held, its files in /data/old", held, moves)
	}
	// Asked and not answering, the config hash stands.
	h.agents.configRefused[pod.Status.PodIP] = true
	h.apply(nil, "")
	if _, held := h.sts("c1").Annotations[DataDirHoldAnnotation]; held {
		t.Fatal("held with the engine not answering and the pod made with this config")
	}
}

// The operator's own KividbDbOps restart, while a `dir` change is held,
// deletes the pod, and the StatefulSet recreates it from the template it
// kept: on its own dir, its data there. The hold stays.
func TestDbOpsRestartWhileADirChangeIsHeld(t *testing.T) {
	ctx := context.Background()
	c := testCluster("c1")
	c.Status.MasterPod = "c1-0"
	c.Status.Pods = []kividbv1alpha1.KividbPodStatus{{Name: "c1-0", Role: kividbv1alpha1.RoleMaster, Ready: true, Synced: true}}
	h := newHoldHarness(t, c)
	if err := h.r.Status().Update(ctx, c); err != nil {
		t.Fatal(err)
	}
	h.apply(dirConfig("sub"), "")
	pods := h.makePods()
	h.apply(nil, "") // dir removed: held
	if _, held := h.sts("c1").Annotations[DataDirHoldAnnotation]; !held {
		t.Fatal("not held")
	}

	op := &kividbv1alpha1.KividbDbOps{
		ObjectMeta: metav1.ObjectMeta{Name: "restart", Namespace: c.Namespace},
		Spec:       kividbv1alpha1.KividbDbOpsSpec{ClusterRef: corev1.LocalObjectReference{Name: c.Name}, Op: kividbv1alpha1.DbOpsRestart},
	}
	if err := h.r.Create(ctx, op); err != nil {
		t.Fatal(err)
	}
	ops := &KividbDbOpsReconciler{Client: h.r.Client, Scheme: h.r.Scheme}
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(op)}
	for i := 0; i < 4; i++ {
		if _, err := ops.Reconcile(ctx, req); err != nil {
			t.Fatal(err)
		}
		var p corev1.Pod
		if err := h.r.Get(ctx, client.ObjectKeyFromObject(&pods[0]), &p); err != nil {
			break // deleted for the restart
		}
		if i == 3 {
			t.Fatal("the restart never deleted c1-0")
		}
	}

	recreated := h.recreate(h.sts("c1"), "c1-0", pods[0].Status.PodIP)
	if got := podDataDir(&recreated.Spec); got != "/data/sub" {
		t.Fatalf("restarted pod on %s, want /data/sub, where its files are", got)
	}
	h.apply(nil, "")
	if _, held := h.sts("c1").Annotations[DataDirHoldAnnotation]; !held {
		t.Fatal("hold gone after the restart")
	}
}

// Without a `dir` in a strict subdirectory of /data, the pod template is
// what it was before #112, so an operator upgrade restarts none of those
// clusters: no init container, no --dir, no ENGINE_DIR for the agent, and
// the same template as with no `dir` at all.
func TestNoSubdirectoryDirLeavesThePodTemplateAsItWas(t *testing.T) {
	for _, c := range []*kividbv1alpha1.KividbCluster{testCluster("r"), shardedCluster("s", 2, 1)} {
		render := func(cfg *kividbv1alpha1.KividbConfig) corev1.PodTemplateSpec {
			if isSharded(c) {
				return shardPodTemplate(c, 0, cfg, nil, nil)
			}
			return podTemplate(c, cfg, nil, nil)
		}
		base := render(directivesConfig(map[string]string{"loglevel": "verbose"}))
		for _, dir := range []string{"", ".", "/data", "/data/", `""`, "./a/.."} {
			d := map[string]string{"loglevel": "verbose"}
			if dir != "" {
				d["dir"] = dir
			}
			got := render(directivesConfig(d))
			if !apiequality.Semantic.DeepEqual(got, base) {
				t.Errorf("%s, dir %q: pod template differs from the one with no dir", c.Name, dir)
			}
			if len(got.Spec.InitContainers) != 0 {
				t.Errorf("%s, dir %q: init containers %+v", c.Name, dir, got.Spec.InitContainers)
			}
			for _, ct := range got.Spec.Containers {
				if _, ok := argValue(ct.Args, "--dir"); ok {
					t.Errorf("%s, dir %q: %s has --dir", c.Name, dir, ct.Name)
				}
				for _, e := range ct.Env {
					if e.Name == engineDirEnv {
						t.Errorf("%s, dir %q: %s has %s", c.Name, dir, ct.Name, engineDirEnv)
					}
				}
			}
		}
		// A subdirectory: the engine's --dir, the init container, and the
		// agent's ENGINE_DIR, all the same directory.
		sub := render(dirConfig("sub"))
		if d, ok := specDataDir(&sub.Spec); !ok || d != "/data/sub" {
			t.Errorf("%s, dir sub: spec dir %q %v", c.Name, d, ok)
		}
		var agentDir string
		for _, ct := range sub.Spec.Containers {
			if ct.Name == kividbContainerName {
				if v, _ := argValue(ct.Args, "--dir"); v != "/data/sub" {
					t.Errorf("%s, dir sub: engine --dir %q", c.Name, v)
				}
			}
			for _, e := range ct.Env {
				if ct.Name == "agent" && e.Name == engineDirEnv {
					agentDir = e.Value
				}
			}
		}
		if agentDir != "/data/sub" {
			t.Errorf("%s, dir sub: agent %s %q, want /data/sub", c.Name, engineDirEnv, agentDir)
		}
	}
}
