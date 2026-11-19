package controller

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

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
// (#112). Its passes run what a reconcile pass runs for it, in its order:
// planDataDir, the config file, the StatefulSets.
type holdHarness struct {
	t      *testing.T
	c      *kividbv1alpha1.KividbCluster
	r      *KividbClusterReconciler
	agents *fakeAgents
	ips    int
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
	agents := &fakeAgents{status: map[string]*agentapi.StatusResponse{}, dirs: map[string]string{}, configRefused: map[string]bool{},
		dirErr: map[string]string{}, hang: map[string]bool{}}
	r := &KividbClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(c).
			WithStatusSubresource(&kividbv1alpha1.KividbCluster{}, &kividbv1alpha1.KividbDbOps{}).Build(),
		Scheme:   scheme,
		Agent:    &AgentClient{http: &http.Client{Transport: agents}},
		Recorder: record.NewFakeRecorder(100),
	}
	t.Cleanup(r.engineDirs.wait)
	return &holdHarness{t: t, c: c, r: r, agents: agents}
}

func (h *holdHarness) desired(cfg *kividbv1alpha1.KividbConfig) []*appsv1.StatefulSet {
	if !isSharded(h.c) {
		return []*appsv1.StatefulSet{desiredStatefulSet(h.c, cfg, nil, nil)}
	}
	var out []*appsv1.StatefulSet
	for s := int32(0); s < shardCount(h.c); s++ {
		out = append(out, desiredShardStatefulSet(h.c, s, cfg, nil, nil))
	}
	return out
}

// passOnce is one pass for the KividbConfig cfg: what to hold, the config
// file, the StatefulSets, and the hold reported. It does not wait for the
// engines it asks.
func (h *holdHarness) passOnce(cfg *kividbv1alpha1.KividbConfig) *dataDirPlan {
	h.t.Helper()
	ctx := context.Background()
	var targets []stsTarget
	for _, sts := range h.desired(cfg) {
		targets = append(targets, targetOf(sts))
	}
	plan, err := h.r.planDataDir(ctx, h.c, cfg, targets)
	if err != nil {
		h.t.Fatal(err)
	}
	if err := h.r.reconcileConfig(ctx, h.c, plan.config, clusterSecrets{}); err != nil {
		h.t.Fatal(err)
	}
	rollout := map[string]string{ConfigHashAnnotation: configHash(renderKividbConf(h.c, plan.config))[:16], AuthGenerationAnnotation: "1"}
	for _, sts := range h.desired(plan.config) {
		if err := h.r.applyStatefulSet(ctx, h.c, sts, rollout, plan); err != nil {
			h.t.Fatal(err)
		}
	}
	if plan.held() {
		h.r.rolloutHeld(h.c, dataDirHoldMessage(h.c, plan.moves, true))
	}
	return plan
}

// pass is two passes, the second once the engines the first asked have
// answered.
func (h *holdHarness) pass(cfg *kividbv1alpha1.KividbConfig) *dataDirPlan {
	h.t.Helper()
	h.passOnce(cfg)
	h.r.engineDirs.wait()
	return h.passOnce(cfg)
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
	var names []string
	for _, sts := range h.desired(nil) {
		names = append(names, sts.Name)
	}
	return names
}

// makePods makes every pod of every StatefulSet from its template, as the
// StatefulSet controller does, Ready, at an address whose agent answers,
// with its volume.
func (h *holdHarness) makePods() []corev1.Pod {
	h.t.Helper()
	var pods []corev1.Pod
	for _, name := range h.stsNames() {
		sts := h.sts(name)
		for i := int32(0); i < *sts.Spec.Replicas; i++ {
			h.ips++
			ip := fmt.Sprintf("10.0.0.%d", h.ips)
			h.agents.status[ip] = &agentapi.StatusResponse{Role: agentapi.RoleMaster}
			pods = append(pods, *h.recreate(sts, fmt.Sprintf("%s-%d", name, i), ip))
		}
	}
	return pods
}

// recreate makes pod name from sts's template, as the StatefulSet
// controller does when a pod is gone: a drain, an eviction, `kubectl
// delete pod` or a KividbDbOps restart. Its volume is made if it is not
// there.
func (h *holdHarness) recreate(sts *appsv1.StatefulSet, name, ip string) *corev1.Pod {
	h.t.Helper()
	ctx := context.Background()
	_ = h.r.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: h.c.Namespace}})
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
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-" + name, Namespace: h.c.Namespace}}
	_ = h.r.Create(ctx, pvc)
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
}

// startDir is the directory pod's engine changes into when it starts (in
// a new pod or restarting in its pod): its --dir, or else the `dir` of the
// config file it reads, as that file is stored now.
func (h *holdHarness) startDir(pod *corev1.Pod) string {
	h.t.Helper()
	for _, ct := range pod.Spec.Containers {
		if ct.Name == kividbContainerName {
			if v, ok := argValue(ct.Args, "--dir"); ok {
				return v
			}
		}
	}
	lines, ok, err := h.r.storedDirLines(context.Background(), h.c)
	if err != nil || !ok {
		h.t.Fatalf("config file: %v, there: %v", err, ok)
	}
	return engineDataDir(dirLinesConfig(lines))
}

// legacy makes the cluster as operator 0.7.1 left it, with the KividbConfig
// dir: the pod template has no init container and no --dir, the config
// file has `dir dir`, made by hand, and the StatefulSet no record of its
// dir. Its pods' engines say they are in it.
func (h *holdHarness) legacy(dir string) []corev1.Pod {
	h.t.Helper()
	ctx := context.Background()
	h.pass(nil)
	for _, name := range h.stsNames() {
		sts := h.sts(name)
		delete(sts.Annotations, DataDirAnnotation)
		if err := h.r.Update(ctx, sts); err != nil {
			h.t.Fatal(err)
		}
	}
	if err := h.r.reconcileConfig(ctx, h.c, dirConfig(dir), clusterSecrets{}); err != nil {
		h.t.Fatal(err)
	}
	pods := h.makePods()
	for _, p := range pods {
		h.agents.dirs[p.Status.PodIP] = engineDataDir(dirConfig(dir))
	}
	return pods
}

func (h *holdHarness) rolloutCondition() *metav1.Condition {
	return findCondition(h.c.Status.Conditions, RolloutProgressingCondition)
}

// A `dir` change held on a cluster with no replica keeps the StatefulSet's
// pod template and the config file's `dir` as they are (#112): a pod
// recreated while it is held (a drain, an eviction, `kubectl delete pod`),
// or one whose engine restarts in its pod, starts in the directory its
// files are in, not an empty new one, and the rollout says why it holds.
// Set back, the template is applied again and the hold is gone.
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
			h.pass(dirConfig(tc.from))
			before := h.sts("c1").Spec.Template.DeepCopy()
			pods := h.makePods()
			from := engineDataDir(dirConfig(tc.from))
			if got := h.startDir(&pods[0]); got != from {
				t.Fatalf("pod starts in %s, want %s", got, from)
			}

			h.pass(dirConfig(tc.to))
			sts := h.sts("c1")
			if !apiequality.Semantic.DeepEqual(sts.Spec.Template, *before) {
				t.Fatalf("template changed while held:\n%+v\nwant\n%+v", sts.Spec.Template, *before)
			}
			if hold := sts.Annotations[DataDirHoldAnnotation]; !strings.Contains(hold, `"c1-0"`) {
				t.Fatalf("hold annotation %q, want c1-0 in it", hold)
			}
			if rec := sts.Annotations[DataDirAnnotation]; rec != from {
				t.Fatalf("record %q, want %q, the dir still applied", rec, from)
			}
			// The engine restarting in its pod reads the config file: its dir
			// is kept too.
			if got := h.startDir(&pods[0]); got != from {
				t.Fatalf("held: the pod's engine would start in %s, want %s, where its files are", got, from)
			}
			// Recreated meanwhile, it starts where its files are.
			recreated := h.recreate(sts, "c1-0", pods[0].Status.PodIP)
			if got := h.startDir(recreated); got != from {
				t.Fatalf("recreated pod starts in %s, want %s, where its files are", got, from)
			}

			h.r.reconcileRollout(ctx, c, []corev1.Pod{*recreated}, []kividbv1alpha1.KividbPodStatus{{Name: "c1-0", Role: kividbv1alpha1.RoleMaster, Ready: true, Synced: true}}, "c1-0")
			cond := h.rolloutCondition()
			if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "WouldLoseData" ||
				!strings.Contains(cond.Message, "c1-0 from "+from) || !strings.Contains(cond.Message, "keeps its current pod template and the config file its dir") || strings.Contains(cond.Message, "delete") {
				t.Fatalf("RolloutProgressing %+v, want WouldLoseData, naming c1-0 and %s, saying what is kept, and not offering to delete the pod", cond, from)
			}

			h.pass(dirConfig(tc.from))
			sts = h.sts("c1")
			if _, held := sts.Annotations[DataDirHoldAnnotation]; held {
				t.Fatalf("dir set back: still held: %q", sts.Annotations[DataDirHoldAnnotation])
			}
			if got := podDataDir(&sts.Spec.Template.Spec); got != from {
				t.Fatalf("dir set back: template dir %s, want %s", got, from)
			}
		})
	}
}

// Sequence A: a cluster from operator 0.7.1 with no replica whose engine
// runs in a `dir` made by hand. With `dir` removed, the move is held, and
// the config file keeps `dir sub`: an engine that starts again, in a new
// pod or in its own, starts in /data/sub, its files there, not in an
// empty /data that the hold would then take for where they are.
func TestHeldDirRemovedFromAHandMadeDir(t *testing.T) {
	h := newHoldHarness(t, testCluster("c1"))
	pods := h.legacy("sub")
	before := h.sts("c1").Spec.Template.DeepCopy()
	for i := 0; i < 3; i++ {
		plan := h.pass(nil)
		if !plan.held() {
			t.Fatalf("pass %d: dir removed from a pod in /data/sub: not held", i)
		}
	}
	sts := h.sts("c1")
	if !apiequality.Semantic.DeepEqual(sts.Spec.Template, *before) {
		t.Fatal("template applied: the pod would be rolled into /data, where its files are not")
	}
	if got := h.startDir(&pods[0]); got != "/data/sub" {
		t.Fatalf("held: an engine restarting in its pod starts in %s, want /data/sub", got)
	}
	recreated := h.recreate(sts, "c1-0", pods[0].Status.PodIP)
	if got := h.startDir(recreated); got != "/data/sub" {
		t.Fatalf("held: a recreated pod starts in %s, want /data/sub", got)
	}
	if cond := h.rolloutCondition(); cond == nil || !strings.Contains(cond.Message, "c1-0 from /data/sub to /data") {
		t.Fatalf("condition %+v, want it to say c1-0's files are in /data/sub", cond)
	}
	// The other directives still go into the config file.
	h.pass(directivesConfig(map[string]string{"maxmemory": "1gb"}))
	var s corev1.Secret
	if err := h.r.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: configSecretName(h.c)}, &s); err != nil {
		t.Fatal(err)
	}
	conf := string(s.Data[ConfigFileName]) + s.StringData[ConfigFileName]
	if !strings.Contains(conf, "maxmemory 1gb\n") || !strings.Contains(conf, "dir sub\n") {
		t.Fatalf("config file:\n%s\nwant maxmemory 1gb, and dir sub kept", conf)
	}
}

// Sequence B: a cluster with no `dir` and no replica; `dir: sub` added and
// held. The config file keeps no `dir`, so an engine that starts again
// starts in /data, where its files are: it neither fails on a /data/sub
// that is not there, nor, where /data/sub is left over from an earlier
// use, starts Ready on the files left there (after which the hold would
// take it for moved).
func TestHeldDirAddedKeepsTheConfigWithoutIt(t *testing.T) {
	for _, leftover := range []bool{false, true} {
		t.Run(fmt.Sprintf("leftover %v", leftover), func(t *testing.T) {
			h := newHoldHarness(t, testCluster("c1"))
			h.pass(nil)
			pods := h.makePods()
			if leftover {
				// Whatever /data/sub holds, an engine that starts there says so.
				h.agents.dirs[pods[0].Status.PodIP] = "/data/sub"
			}
			if plan := h.pass(dirConfig("sub")); !plan.held() {
				t.Fatal("dir added: not held")
			}
			sts := h.sts("c1")
			for _, p := range []*corev1.Pod{&pods[0], h.recreate(sts, "c1-0", pods[0].Status.PodIP)} {
				if got := h.startDir(p); got != "/data" {
					t.Fatalf("held: the engine starts in %s, want /data, where its files are", got)
				}
			}
			if plan := h.pass(dirConfig("sub")); !plan.held() {
				t.Fatal("held once, then released")
			}
		})
	}
}

// A cluster with no `dir` is never held for a change to another directive
// (#112): its pods are in /data by the StatefulSet's record, and no engine
// is asked, even one that refuses CONFIG or a pod that has no IP yet. A
// cluster an earlier operator made is the same: its config file has no
// `dir` either.
func TestNoDirClusterIsNotHeldForAChangeElsewhere(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, sharded := range []bool{false, true} {
			t.Run(fmt.Sprintf("legacy %v sharded %v", legacy, sharded), func(t *testing.T) {
				c := testCluster("c1")
				if sharded {
					c = shardedCluster("s", 2, 1)
				}
				h := newHoldHarness(t, c)
				var pods []corev1.Pod
				if legacy {
					pods = h.legacy("")
				} else {
					h.pass(nil)
					pods = h.makePods()
				}
				h.agents.configRefused[pods[0].Status.PodIP] = true
				pods[1%len(pods)].Status.PodIP = ""
				h.writePodStatus(&pods[1%len(pods)])

				cfg := directivesConfig(map[string]string{"maxmemory": "1gb"})
				h.c.Spec.Image = "kividb:new"
				if plan := h.pass(cfg); plan.held() {
					t.Fatalf("held: %+v", plan.moves)
				}
				sts := h.sts(h.stsNames()[0])
				if _, held := sts.Annotations[DataDirHoldAnnotation]; held {
					t.Fatalf("held: %q", sts.Annotations[DataDirHoldAnnotation])
				}
				if sts.Spec.Template.Spec.Containers[0].Image != "kividb:new" || sts.Spec.Template.Annotations[ConfigHashAnnotation] != configHash(renderKividbConf(h.c, cfg))[:16] {
					t.Fatal("template not applied")
				}
				if h.agents.dirAsks != 0 {
					t.Fatalf("%d engines asked, want none", h.agents.dirAsks)
				}
			})
		}
	}
}

// A replica added to go on is made from the template kept, in the
// master's directory, and the hold ends only once it is Ready and in sync:
// until then the master's pod could be recreated from the new template
// (#112).
func TestHeldDirChangeWaitsForAReplicaInSync(t *testing.T) {
	c := testCluster("c1")
	h := newHoldHarness(t, c)
	h.pass(dirConfig("sub"))
	h.makePods()
	before := h.sts("c1").Spec.Template.DeepCopy()
	if plan := h.pass(nil); !plan.held() {
		t.Fatal("not held")
	}

	c.Spec.Replicas = 1
	if plan := h.pass(nil); !plan.held() {
		t.Fatal("replica asked for, none in sync yet: not held")
	}
	sts := h.sts("c1")
	if *sts.Spec.Replicas != 2 || !apiequality.Semantic.DeepEqual(sts.Spec.Template, *before) {
		t.Fatalf("replicas %d, template kept %v; want 2 and kept", *sts.Spec.Replicas, apiequality.Semantic.DeepEqual(sts.Spec.Template, *before))
	}
	replica := h.recreate(sts, "c1-1", "10.0.0.9")
	h.agents.status["10.0.0.9"] = &agentapi.StatusResponse{Role: agentapi.RoleReplica}
	if got := h.startDir(replica); got != "/data/sub" {
		t.Fatalf("replica starts in %s, want /data/sub, the master's", got)
	}
	cond := h.rolloutCondition()
	if cond == nil || !strings.Contains(cond.Message, "once a replica is Ready and in sync") {
		t.Fatalf("condition %+v", cond)
	}

	c.Status.Pods = []kividbv1alpha1.KividbPodStatus{
		{Name: "c1-0", Role: kividbv1alpha1.RoleMaster, Ready: true, Synced: true},
		{Name: "c1-1", Role: kividbv1alpha1.RoleReplica, Ready: true, Synced: false},
	}
	if plan := h.pass(nil); !plan.held() {
		t.Fatal("replica not in sync: not held")
	}
	c.Status.Pods[1].Synced = true
	if plan := h.pass(nil); plan.held() {
		t.Fatalf("replica in sync: still held: %+v", plan.moves)
	}
	if got := podDataDir(&h.sts("c1").Spec.Template.Spec); got != "/data" {
		t.Fatalf("template dir %s, want /data applied", got)
	}
}

// A StatefulSet that is not there, its pods orphaned (`kubectl delete sts
// --cascade=orphan`) or only its volumes left, is not made while a move is
// held: made then, it would be made from the new template. One scaled to 0
// is held for the pods it would make again (#112).
func TestHeldDirChangeWithTheStatefulSetGoneOrScaledDown(t *testing.T) {
	setup := func(t *testing.T) *holdHarness {
		h := newHoldHarness(t, testCluster("c1"))
		h.pass(dirConfig("sub"))
		h.makePods()
		return h
	}
	ctx := context.Background()
	t.Run("orphaned pods", func(t *testing.T) {
		h := setup(t)
		if err := h.r.Delete(ctx, h.sts("c1")); err != nil {
			t.Fatal(err)
		}
		if plan := h.pass(nil); !plan.held() {
			t.Fatal("not held")
		}
		var sts appsv1.StatefulSet
		if err := h.r.Get(ctx, client.ObjectKey{Namespace: "default", Name: "c1"}, &sts); err == nil {
			t.Fatalf("StatefulSet made while held, template dir %s", podDataDir(&sts.Spec.Template.Spec))
		}
		if cond := h.rolloutCondition(); cond == nil || cond.Reason != "WouldLoseData" {
			t.Fatalf("condition %+v", cond)
		}
		h.pass(dirConfig("sub"))
		if got := podDataDir(&h.sts("c1").Spec.Template.Spec); got != "/data/sub" {
			t.Fatalf("dir set back: made with %s", got)
		}
	})
	t.Run("only the volume left", func(t *testing.T) {
		h := setup(t)
		if err := h.r.Delete(ctx, h.sts("c1")); err != nil {
			t.Fatal(err)
		}
		if err := h.r.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "c1-0", Namespace: "default"}}); err != nil {
			t.Fatal(err)
		}
		plan := h.pass(nil)
		if !plan.held() || plan.moves[0].Why != whyNoPod {
			t.Fatalf("plan %+v, want c1-0 held for its volume", plan.moves)
		}
	})
	t.Run("scaled to 0", func(t *testing.T) {
		h := setup(t)
		sts := h.sts("c1")
		zero := int32(0)
		sts.Spec.Replicas = &zero
		if err := h.r.Update(ctx, sts); err != nil {
			t.Fatal(err)
		}
		if err := h.r.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "c1-0", Namespace: "default"}}); err != nil {
			t.Fatal(err)
		}
		before := sts.Spec.Template.DeepCopy()
		if plan := h.pass(nil); !plan.held() {
			t.Fatal("scaled to 0, its volume there: not held")
		}
		if !apiequality.Semantic.DeepEqual(h.sts("c1").Spec.Template, *before) {
			t.Fatal("template applied")
		}
	})
	t.Run("more pods there than asked for", func(t *testing.T) {
		// The StatefulSet has 2 pods, c1-1 gone with its volume left, and
		// the spec asks for fewer: c1-1 counts until the StatefulSet has
		// fewer too.
		h := newHoldHarness(t, testCluster("c1"))
		h.c.Spec.Replicas = 1
		h.c.Status.Pods = []kividbv1alpha1.KividbPodStatus{{Name: "c1-1", Role: kividbv1alpha1.RoleReplica, Ready: true, Synced: true}}
		h.pass(dirConfig("sub"))
		h.makePods()
		if err := h.r.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "c1-1", Namespace: "default"}}); err != nil {
			t.Fatal(err)
		}
		h.c.Spec.Replicas = 0
		h.c.Status.Pods = nil
		plan := h.passOnce(nil) // the StatefulSet still has 2
		var gone bool
		for _, m := range plan.moves {
			gone = gone || (m.Pod == "c1-1" && m.Why == whyNoPod)
		}
		if !gone {
			t.Fatalf("plan %+v, want c1-1 held for its volume", plan.moves)
		}
	})
	t.Run("a new cluster is made", func(t *testing.T) {
		h := newHoldHarness(t, testCluster("c1"))
		if plan := h.pass(dirConfig("sub")); plan.held() {
			t.Fatalf("held: %+v", plan.moves)
		}
		h.sts("c1")
	})
}

// A sharded cluster holds a `dir` change with replicas or not: every
// shard's StatefulSet keeps its template, and a pod recreated meanwhile
// starts on its own dir, its nodes.conf with it (#112).
func TestHeldDirChangeKeepsTheShardTemplates(t *testing.T) {
	c := shardedCluster("s", 2, 1)
	h := newHoldHarness(t, c)
	h.pass(dirConfig("sub"))
	before := map[string]*corev1.PodTemplateSpec{}
	for _, name := range h.stsNames() {
		before[name] = h.sts(name).Spec.Template.DeepCopy()
	}
	h.makePods()
	h.pass(nil)
	for _, name := range h.stsNames() {
		sts := h.sts(name)
		if !apiequality.Semantic.DeepEqual(sts.Spec.Template, *before[name]) {
			t.Fatalf("%s: template changed while held", name)
		}
		if hold := sts.Annotations[DataDirHoldAnnotation]; !strings.Contains(hold, `"`+name+`-0"`) || !strings.Contains(hold, `"`+name+`-1"`) {
			t.Fatalf("%s: hold annotation %q, want both its pods", name, hold)
		}
		if p := h.recreate(sts, name+"-0", "10.0.9.1"); h.startDir(p) != "/data/sub" {
			t.Fatalf("%s-0 recreated on %s, want /data/sub", name, h.startDir(p))
		}
	}
	h.pass(dirConfig("sub"))
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

// A pod made by operator 0.7.1 has no data-dir init container: where its
// files are is asked of its engine (#112). One whose engine says it is
// where the KividbConfig puts it is rolled onto the template that says so;
// one whose engine cannot be asked is held, and the message says the
// operator could not ask, and what to do.
func TestALegacyPodIsAskedWhereItsFilesAre(t *testing.T) {
	t.Run("dir kept: the template takes it over", func(t *testing.T) {
		h := newHoldHarness(t, testCluster("c1"))
		pods := h.legacy("sub")
		if plan := h.pass(dirConfig("sub")); plan.held() {
			t.Fatalf("held: %+v", plan.moves)
		}
		sts := h.sts("c1")
		if podDataDir(&sts.Spec.Template.Spec) != "/data/sub" || sts.Annotations[DataDirAnnotation] != "/data/sub" {
			t.Fatalf("template dir %s, record %q; want /data/sub", podDataDir(&sts.Spec.Template.Spec), sts.Annotations[DataDirAnnotation])
		}
		// The rollout, replacing it, finds it where its replacement looks.
		h.r.dataDirMoved(context.Background(), &pods[0], sts)
		h.r.engineDirs.wait()
		if m, moved := h.r.dataDirMoved(context.Background(), &pods[0], sts); moved {
			t.Fatalf("rollout sees a move %+v, want none", m)
		}
	})
	for _, tc := range []struct{ name, why string }{
		{"CONFIG denied", whyDenied},
		{"no answer", whyNoAnswer},
		{"no pod IP", whyNoIP},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHoldHarness(t, testCluster("c1"))
			pods := h.legacy("sub")
			ip := pods[0].Status.PodIP
			switch tc.why {
			case whyDenied:
				h.agents.configRefused[ip] = true
			case whyNoAnswer:
				h.agents.dirErr[ip] = "dial tcp: connection refused"
			case whyNoIP:
				pods[0].Status.PodIP = ""
				h.writePodStatus(&pods[0])
			}
			before := h.sts("c1").Spec.Template.DeepCopy()
			plan := h.pass(nil)
			if !plan.held() || plan.moves[0].Why != tc.why {
				t.Fatalf("plan %+v, want c1-0 held: %s", plan.moves, tc.why)
			}
			if !apiequality.Semantic.DeepEqual(h.sts("c1").Spec.Template, *before) {
				t.Fatal("template applied with the pod's dir unknown")
			}
			cond := h.rolloutCondition()
			for _, want := range []string{"c1-0 to /data, from where is not known (" + tc.why + ")", "The operator could not ask where the files of c1-0 are", "CONFIG GET dir as a user allowed CONFIG", "Where a pod keeps its files", "set dir to that"} {
				if cond == nil || !strings.Contains(cond.Message, want) {
					t.Fatalf("condition %+v, want %q in its message", cond, want)
				}
			}
		})
	}
}

// Engines are asked in the background, all at once (#112): a pass does not
// wait for one that hangs, so failover, later in the same pass, is not
// held up, and the answer, once there, is used without asking again.
func TestEnginesAreAskedWithoutWaiting(t *testing.T) {
	h := newHoldHarness(t, shardedCluster("s", 2, 1))
	pods := h.legacy("sub")
	for _, p := range pods {
		h.agents.hang[p.Status.PodIP] = true
	}
	for i := 0; i < 2; i++ {
		start := time.Now()
		plan := h.passOnce(nil)
		if took := time.Since(start); took > time.Second {
			t.Fatalf("pass %d took %s with every engine hanging, want no wait", i, took)
		}
		if !plan.held() || plan.moves[0].Why != whyAsking {
			t.Fatalf("pass %d: plan %+v, want held while asking", i, plan.moves)
		}
	}
	h.r.engineDirs.wait()
	if n := len(h.r.engineDirs.answers); n != len(pods) {
		t.Fatalf("%d asks, want one per pod: %d", n, len(pods))
	}
	for _, p := range pods {
		h.agents.hang[p.Status.PodIP] = false
	}
	engineDirRetry = 0
	t.Cleanup(func() { engineDirRetry = 15 * time.Second })
	h.pass(nil) // asks again: the hanging ones gave no answer
	plan := h.pass(nil)
	if !plan.held() || plan.moves[0].Why != "" || plan.moves[0].From != "/data/sub" {
		t.Fatalf("plan %+v, want held, from /data/sub", plan.moves)
	}
	asked := h.agents.dirAsks
	h.passOnce(nil)
	h.r.engineDirs.wait()
	if h.agents.dirAsks != asked {
		t.Fatalf("asked again (%d, was %d) with the answers there", h.agents.dirAsks, asked)
	}
}

// What the hold says, and its annotation, stay as they are while the error
// behind a pod's unknown dir changes from pass to pass: no new Event, no
// write of the StatefulSet (#112).
func TestHeldDirChangeIsStableWhileTheErrorChanges(t *testing.T) {
	engineDirRetry = 0
	t.Cleanup(func() { engineDirRetry = 15 * time.Second })
	h := newHoldHarness(t, testCluster("c1"))
	pods := h.legacy("sub")
	ip := pods[0].Status.PodIP
	h.agents.dirErr[ip] = "read tcp: i/o timeout"
	h.pass(nil)
	_ = events(h.r)
	rv := h.sts("c1").ResourceVersion
	for i, e := range []string{"connection reset by peer", "dial tcp: connection refused", "read tcp: i/o timeout"} {
		h.r.engineDirs.wait()
		h.agents.mu.Lock()
		h.agents.dirErr[ip] = e
		h.agents.mu.Unlock()
		h.pass(nil)
		if got := h.sts("c1").ResourceVersion; got != rv {
			t.Fatalf("pass %d: StatefulSet written again (%s, was %s)", i, got, rv)
		}
	}
	if ev := events(h.r); strings.Contains(ev, "RolloutHeld") {
		t.Fatalf("events while only the error changed:\n%s", ev)
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
	h.pass(dirConfig("sub"))
	pods := h.makePods()
	if plan := h.pass(nil); !plan.held() {
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
	if got := h.startDir(recreated); got != "/data/sub" {
		t.Fatalf("restarted pod starts in %s, want /data/sub, where its files are", got)
	}
	if plan := h.pass(nil); !plan.held() {
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
