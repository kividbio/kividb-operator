package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// A KividbConfig `dir` change moves where the engine keeps its snapshot,
// append-only file and, in a sharded cluster, nodes.conf (#112). A pod
// started on a new, empty `dir` comes up Ready and empty: the scheduled
// backups would then upload the empty dataset, and in time prune the good
// ones, and a sharded node would come back as a new node. So on a cluster
// that has no replica to resync from (replicas: 0) and on every sharded
// cluster, a change that would move a pod's files is held:
//
//   - The StatefulSet keeps its current pod template, all of it: the pods
//     are OnDelete, and one recreated another way than by the rollout (a
//     node drain, an eviction, node loss, `kubectl delete pod`, a
//     KividbDbOps restart) is made from the template, so it comes back as
//     it was. Only the parts that decide the data location could be held
//     back instead, but which those are is not all in the template (a pod
//     made by operator 0.7.1 or earlier takes `dir` from its config file),
//     and a pod rolled onto the rest would restart for a change that is
//     held. Other changes to the template (an image, a resource) wait too.
//   - The rollout replaces no pod, and says why on RolloutProgressing
//     (WouldLoseData).
//
// The StatefulSet carries the pods held, and where their files are and
// would go, in DataDirHoldAnnotation, for the rollout to report.

// DataDirHoldAnnotation, on a StatefulSet (not its pod template), lists
// the pods whose files a held `dir` change would move, as JSON (see
// dataDirMove). It is there only while the change is held.
const DataDirHoldAnnotation = "kividb.io/data-dir-hold"

// engineDirAskTimeout bounds asking an engine where it keeps its files.
const engineDirAskTimeout = 3 * time.Second

// dataDirMove is a pod whose replacement would have the engine keep its
// files in another directory than it does: a KividbConfig `dir` added,
// changed or removed (#112). From is "" when where they are is not known,
// and Why then says why.
type dataDirMove struct {
	Pod   string `json:"pod"`
	From  string `json:"from,omitempty"`
	To    string `json:"to"`
	Why   string `json:"why,omitempty"`
	Ready bool   `json:"ready,omitempty"`
}

// podEngineDir is the directory pod's engine keeps its files in, or "" and
// why that is not known (#112).
//
// A pod whose spec says (the data-dir init container, the engine's --dir)
// is there. One whose spec does not, a pod made by operator 0.7.1 or
// earlier or one whose `dir` is DataDir, has its engine take `dir` from
// its config file, as that file was when the engine last started. That is
// DataDir when the pod was made with the config the template to compare
// with has (its config hash, wantHash) and that config has no other
// `dir` (want is DataDir): only when trustHash, as for a template this
// operator rendered for that config, and, for an engine that restarted in
// its pod since, only when it cannot be asked. Otherwise the engine is asked
// (CONFIG GET dir, through the agent): a `dir` made by hand on a 0.7.1
// cluster is not DataDir, and taking it for that would roll the pod into
// an empty /data. An engine that cannot be asked (not running, CONFIG
// refused) leaves it unknown.
func (r *KividbClusterReconciler) podEngineDir(ctx context.Context, pod *corev1.Pod, want, wantHash string, trustHash bool) (dir, why string) {
	if d, ok := specDataDir(&pod.Spec); ok {
		return d, ""
	}
	if trustHash && want == DataDir && pod.Annotations[ConfigHashAnnotation] == wantHash {
		// An engine that restarted in its pod read the config file as it
		// was then, which may have had a `dir` since changed back: its own
		// word, when it can give it, comes first.
		if engineRestartedInPod(pod) {
			if d, why := r.askEngineDir(ctx, pod); why == "" {
				return d, ""
			}
		}
		return DataDir, ""
	}
	return r.askEngineDir(ctx, pod)
}

// engineRestartedInPod reports whether pod's engine container has restarted in
// the pod.
func engineRestartedInPod(pod *corev1.Pod) bool {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name == kividbContainerName && cs.RestartCount > 0 {
			return true
		}
	}
	return false
}

// askEngineDir asks pod's engine, through its agent, where it keeps its
// files (CONFIG GET dir), or says why it could not tell.
func (r *KividbClusterReconciler) askEngineDir(ctx context.Context, pod *corev1.Pod) (dir, why string) {
	if pod.Status.PodIP == "" {
		return "", "it has no pod IP to ask its engine at"
	}
	if r.Agent == nil {
		return "", "there is no agent client to ask its engine with"
	}
	actx, cancel := context.WithTimeout(ctx, engineDirAskTimeout)
	defer cancel()
	reply, err := r.Agent.Exec(actx, pod.Status.PodIP, "CONFIG", "GET", "dir")
	if err != nil {
		return "", fmt.Sprintf("CONFIG GET dir: %v", err)
	}
	if reply == nil || len(reply.Array) != 2 || reply.Array[1].Str == "" {
		return "", "CONFIG GET dir named no directory"
	}
	d := reply.Array[1].Str
	if !path.IsAbs(d) {
		d = path.Join(DataDir, d)
	}
	return path.Clean(d), ""
}

// dataDirMoved says whether pod's replacement, made from sts's template,
// would have its engine keep its files elsewhere than they are, or where
// they are is not known, and how, as a dataDirMove.
func (r *KividbClusterReconciler) dataDirMoved(ctx context.Context, pod *corev1.Pod, sts *appsv1.StatefulSet) (dataDirMove, bool) {
	to := podDataDir(&sts.Spec.Template.Spec)
	// A template held back (DataDirHoldAnnotation) may be one an older
	// operator wrote, whose pods take `dir` from their config file: its
	// config hash says nothing about where their files are.
	trust := sts.Annotations[DataDirHoldAnnotation] == ""
	from, why := r.podEngineDir(ctx, pod, to, sts.Spec.Template.Annotations[ConfigHashAnnotation], trust)
	m := dataDirMove{Pod: pod.Name, From: from, To: to, Why: why, Ready: isPodReady(pod)}
	return m, why != "" || from != to
}

// holdsDataDir reports whether c holds a `dir` change that would move a
// pod's files: with no replica to resync from, or sharded.
func holdsDataDir(c *kividbv1alpha1.KividbCluster) bool {
	return isSharded(c) || c.Spec.Replicas == 0
}

// dataDirHold is what applying template, with config hash hash, to the
// StatefulSet current would move (#112): the pods of current whose files
// are elsewhere than template has them, or not known to be there, and,
// when current's own template has its pods' files elsewhere than template
// does, those of its pods that are not there, whose recreation would come
// from it. None on a cluster that does not hold such a change.
func (r *KividbClusterReconciler) dataDirHold(ctx context.Context, c *kividbv1alpha1.KividbCluster, current *appsv1.StatefulSet, template *corev1.PodTemplateSpec, hash string) ([]dataDirMove, error) {
	if !holdsDataDir(c) {
		return nil, nil
	}
	var pods corev1.PodList
	var match client.MatchingLabels
	if current.Spec.Selector != nil {
		match = current.Spec.Selector.MatchLabels
	}
	if len(match) == 0 {
		return nil, fmt.Errorf("StatefulSet %s has no selector to find its pods by", current.Name)
	}
	if err := r.List(ctx, &pods, client.InNamespace(current.Namespace), match); err != nil {
		return nil, fmt.Errorf("listing the pods of StatefulSet %s: %w", current.Name, err)
	}
	to := podDataDir(&template.Spec)
	var moves []dataDirMove
	seen := map[string]bool{}
	for i := range pods.Items {
		p := &pods.Items[i]
		seen[p.Name] = true
		from, why := r.podEngineDir(ctx, p, to, hash, true)
		if why != "" || from != to {
			moves = append(moves, dataDirMove{Pod: p.Name, From: from, To: to, Why: why, Ready: isPodReady(p)})
		}
	}
	if podDataDir(&current.Spec.Template.Spec) != to && current.Spec.Replicas != nil {
		for i := int32(0); i < *current.Spec.Replicas; i++ {
			if name := fmt.Sprintf("%s-%d", current.Name, i); !seen[name] {
				moves = append(moves, dataDirMove{Pod: name, To: to, Why: "the pod is not there: it is being recreated"})
			}
		}
	}
	sortMoves(moves)
	return moves, nil
}

func sortMoves(moves []dataDirMove) {
	sort.Slice(moves, func(i, j int) bool { return moves[i].Pod < moves[j].Pod })
}

// encodeDataDirHold is moves (at least one) as DataDirHoldAnnotation holds them.
func encodeDataDirHold(moves []dataDirMove) string {
	b, err := json.Marshal(moves)
	if err != nil {
		// A slice of plain structs: it cannot fail.
		panic(err)
	}
	return string(b)
}

// heldDataDirMoves is the moves sts's DataDirHoldAnnotation lists, and
// whether it has one. One that cannot be read holds all the same.
func heldDataDirMoves(sts *appsv1.StatefulSet) ([]dataDirMove, bool) {
	v, ok := sts.Annotations[DataDirHoldAnnotation]
	if !ok {
		return nil, false
	}
	var moves []dataDirMove
	if err := json.Unmarshal([]byte(v), &moves); err != nil || len(moves) == 0 {
		return []dataDirMove{{Pod: sts.Name, Why: fmt.Sprintf("the StatefulSet's %s annotation could not be read", DataDirHoldAnnotation)}}, true
	}
	return moves, true
}

// dataDirMovesText names the pods of moves (at least one), sorted, and
// where their files would move: "c1-0 from /data to /data/sub", or for
// one whose files are not known to be anywhere, "c1-0 to /data/sub, from
// where its engine could not say (why)".
func dataDirMovesText(moves []dataDirMove) string {
	sortMoves(moves)
	type fromTo struct{ from, to string }
	var order []fromTo
	pods := map[fromTo][]string{}
	var unknown []string
	for _, m := range moves {
		if m.Why != "" {
			unknown = append(unknown, fmt.Sprintf("%s to %s, from where is not known (%s)", m.Pod, m.To, m.Why))
			continue
		}
		k := fromTo{m.From, m.To}
		if pods[k] == nil {
			order = append(order, k)
		}
		pods[k] = append(pods[k], m.Pod)
	}
	parts := make([]string, 0, len(order)+len(unknown))
	for _, k := range order {
		parts = append(parts, fmt.Sprintf("%s from %s to %s", strings.Join(pods[k], ", "), k.from, k.to))
	}
	return strings.Join(append(parts, unknown...), "; ")
}

// dataDirHoldMessage says why c's rollout holds the pods of moves (at
// least one), and how to go on. kept: the StatefulSets keep their current
// pod templates meanwhile (DataDirHoldAnnotation).
//
// The way out is to set `dir` back, or, on a replicated cluster, to add a
// replica, which syncs from the master while its engine runs (one that is
// not Ready, whose engine restarted, read the new `dir` from its config
// and cannot start there, has to have `dir` set back). There is no way to
// have a pod start empty on the new `dir`: deleting it brings it back as
// it was. A pod whose files are not known to be anywhere (a pod made by
// operator 0.7.1 or earlier, whose engine could not be asked) is held
// until its engine says, and the message says how to see where they are.
func dataDirHoldMessage(c *kividbv1alpha1.KividbCluster, moves []dataDirMove, kept bool) string {
	var b strings.Builder
	fmt.Fprintf(&b, "the KividbConfig's dir moves the engine's files of %s: ", dataDirMovesText(moves))
	var unknown, unready []string
	for _, m := range moves {
		if m.Why != "" {
			unknown = append(unknown, m.Pod)
		}
		if !m.Ready {
			unready = append(unready, m.Pod)
		}
	}
	if isSharded(c) {
		b.WriteString("a dir change is not supported on a running sharded cluster yet, since each node keeps its nodes.conf there too and would come back as a new node. Set dir back")
	} else {
		replacement := "its replacement"
		if len(moves) > 1 {
			replacement = "their replacements"
		}
		fmt.Fprintf(&b, "with no replica to resync from, %s would start empty, and scheduled backups would upload that. ", replacement)
		switch len(unready) {
		case 0:
			b.WriteString("Add a replica (spec.replicas: 1), which syncs before the master hands over, or set dir back")
		case 1:
			fmt.Fprintf(&b, "Set dir back: %s is not Ready, so a replica would have nothing to sync from", unready[0])
		default:
			fmt.Fprintf(&b, "Set dir back: %s are not Ready, so a replica would have nothing to sync from", strings.Join(unready, ", "))
		}
	}
	if len(unknown) > 0 {
		fmt.Fprintf(&b, ". Where the files of %s are is not known: a pod made by operator 0.7.1 or earlier takes dir from its config file, as it was when its engine started, and the operator could not ask the engine (CONFIG GET dir). See where with CONFIG GET dir as a user allowed CONFIG (TROUBLESHOOTING.md, \"Where a pod keeps its files\") and set dir to that; the operator asks the engine again on every pass",
			strings.Join(unknown, ", "))
	}
	if kept {
		b.WriteString(". Until then the StatefulSet keeps its current pod template, so other changes to it wait too, and a pod recreated meanwhile (a drain, an eviction, a KividbDbOps restart) comes back as it was")
	}
	return b.String()
}
