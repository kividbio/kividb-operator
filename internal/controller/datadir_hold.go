package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// A KividbConfig `dir` change moves where the engine keeps its snapshot,
// append-only file and, in a sharded cluster, nodes.conf (#112). A pod
// started on a new, empty `dir` comes up Ready and empty: the scheduled
// backups would then upload the empty dataset, and in time prune the good
// ones, and a sharded node would come back as a new node. So on a sharded
// cluster, and on a replicated one with no replica Ready and in sync, a
// change that would move a pod's files is held (planDataDir, before any of
// the cluster's objects is written):
//
//   - The StatefulSet keeps its current pod template, all of it, and the
//     config file keeps its current `dir` line (the rest of it is
//     updated). The pods are OnDelete, and one recreated another way than
//     by the rollout (a node drain, an eviction, node loss, `kubectl
//     delete pod`, a KividbDbOps restart) is made from the template and
//     reads the config file, as an engine restarting in its pod does: with
//     both kept it starts in the directory its files are in. Only the parts
//     of the template that decide the data location could be held back
//     instead, but a pod rolled onto the rest would restart for a change
//     that is held. Other changes to the template (an image, a resource)
//     wait too.
//   - A StatefulSet that is not there (deleted with its pods orphaned) is
//     not made until the hold ends: made now, it would be made from the new
//     template.
//   - The rollout replaces no pod, and says why on RolloutProgressing
//     (WouldLoseData).
//
// Each StatefulSet records the `dir` its applied template and config were
// rendered for in DataDirAnnotation, and the pods held in
// DataDirHoldAnnotation, both on its metadata, not its pod template, so
// that neither rolls a pod.

// DataDirHoldAnnotation, on a StatefulSet (not its pod template), lists
// the pods whose files a held `dir` change would move, as JSON (see
// dataDirMove). It is there only while the change is held.
const DataDirHoldAnnotation = "kividb.io/data-dir-hold"

// DataDirAnnotation, on a StatefulSet (not its pod template), is the
// directory its pod template and the config file were last applied for
// (#112). A pod of it whose spec does not say (no data-dir init container,
// no --dir) has its engine read `dir` from that config file: with this
// /data, the pod is in /data. A StatefulSet without it was last applied by
// an operator before it, and its pods are asked.
const DataDirAnnotation = "kividb.io/data-dir"

// engineDirAskTimeout bounds asking an engine where it keeps its files.
const engineDirAskTimeout = 3 * time.Second

// engineDirRetry is how long an engine that could not say is not asked
// again (a variable for tests).
var engineDirRetry = 15 * time.Second

// Why a pod's files are not known to be anywhere: a few fixed reasons, so
// that the hold's message and annotation stay as they are while the error
// behind one changes from pass to pass (the detail is logged).
const (
	whyAsking   = "its engine is being asked (CONFIG GET dir)"
	whyNoIP     = "it has no pod IP, so its engine could not be asked"
	whyDenied   = "CONFIG GET dir is denied to the agent's user"
	whyNoAnswer = "its agent or engine did not answer CONFIG GET dir"
	whyNoDir    = "CONFIG GET dir named no directory"
	whyNoPod    = "its pod is not there, and its volume is"
)

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

// engineDirAsks are the answers of engines asked where they keep their
// files, by pod UID and engine restart count: an engine that restarts may
// have read another config file. Engines are asked in the background, all
// at once, so that a pass never waits on one (failover runs later in the
// same pass): a pass that has no answer yet takes the files as not known.
type engineDirAsks struct {
	mu      sync.Mutex
	answers map[string]engineDirAnswer
	running sync.WaitGroup
}

type engineDirAnswer struct {
	dir, why string
	known    bool // an answer came back (dir, why)
	asking   bool // an ask is under way
	at       time.Time
}

// wait waits for the asks under way (for tests).
func (a *engineDirAsks) wait() { a.running.Wait() }

// engineDir is where pod's engine says it keeps its files, or "" and why
// that is not known (yet). It does not wait: an engine not asked yet is
// asked in the background. One that could not say is asked again after
// engineDirRetry, its last answer standing meanwhile, so that what the
// hold says does not change with every ask.
func (r *KividbClusterReconciler) engineDir(ctx context.Context, pod *corev1.Pod) (dir, why string) {
	if pod.Status.PodIP == "" {
		return "", whyNoIP
	}
	if r.Agent == nil {
		return "", whyNoAnswer
	}
	key := string(pod.UID) + "/" + pod.Name + "/" + strconv.Itoa(int(engineRestarts(pod)))
	a := &r.engineDirs
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.answers == nil {
		a.answers = map[string]engineDirAnswer{}
	}
	ans, ok := a.answers[key]
	switch {
	case ok && ans.known && (ans.why == "" || ans.asking || time.Since(ans.at) < engineDirRetry):
		return ans.dir, ans.why
	case ok && ans.asking:
		return "", whyAsking
	}
	for k, old := range a.answers {
		if !old.asking && time.Since(old.at) > time.Hour {
			delete(a.answers, k)
		}
	}
	ans.asking = true
	a.answers[key] = ans
	ip, name := pod.Status.PodIP, pod.Name
	log := logf.FromContext(ctx)
	a.running.Add(1)
	go func() {
		defer a.running.Done()
		d, why := r.askEngineDir(log, ip, name)
		a.mu.Lock()
		a.answers[key] = engineDirAnswer{dir: d, why: why, known: true, at: time.Now()}
		a.mu.Unlock()
	}()
	if ans.known {
		return ans.dir, ans.why
	}
	return "", whyAsking
}

// engineRestarts is how many times pod's engine container has restarted
// in the pod.
func engineRestarts(pod *corev1.Pod) int32 {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name == kividbContainerName {
			return cs.RestartCount
		}
	}
	return 0
}

// askEngineDir asks the engine at ip, through its agent, where it keeps
// its files (CONFIG GET dir), or says why it could not tell.
func (r *KividbClusterReconciler) askEngineDir(log interface {
	Info(string, ...any)
}, ip, pod string) (dir, why string) {
	ctx, cancel := context.WithTimeout(context.Background(), engineDirAskTimeout)
	defer cancel()
	reply, err := r.Agent.Exec(ctx, ip, "CONFIG", "GET", "dir")
	if err != nil {
		log.Info("could not ask an engine where it keeps its files", "pod", pod, "error", err.Error())
		if strings.Contains(err.Error(), "NOPERM") {
			return "", whyDenied
		}
		return "", whyNoAnswer
	}
	if reply == nil || len(reply.Array) != 2 || reply.Array[1].Str == "" {
		return "", whyNoDir
	}
	d := reply.Array[1].Str
	if !path.IsAbs(d) {
		d = path.Join(DataDir, d)
	}
	return path.Clean(d), ""
}

// podEngineDir is the directory pod's engine keeps its files in, or "" and
// why that is not known (#112). A pod whose spec says (the data-dir init
// container, the engine's --dir) is there. One whose spec does not has
// its engine read `dir` from the config file each time it starts: when its
// StatefulSet records (record, DataDirAnnotation) that its template and
// config were applied for /data, it is in /data, since a pod without --dir
// is made only for that, and the config file's `dir` does not change while
// a move is held. Otherwise (a pod from before that record, as one made by
// operator 0.7.1, whose engine may run in a `dir` made by hand), its
// engine is asked.
func (r *KividbClusterReconciler) podEngineDir(ctx context.Context, pod *corev1.Pod, record string, hasRecord bool) (dir, why string) {
	if d, ok := specDataDir(&pod.Spec); ok {
		return d, ""
	}
	if hasRecord && record == DataDir {
		return DataDir, ""
	}
	return r.engineDir(ctx, pod)
}

// dataDirMoved says whether pod's replacement, made from sts's template,
// would have its engine keep its files elsewhere than they are, or where
// they are is not known, and how, as a dataDirMove.
//
// A pod and a template that neither say (no init container, no --dir)
// both have the engine read `dir` from the same config file: the
// replacement starts where the pod's engine would, restarting in its pod,
// and nothing is asked.
func (r *KividbClusterReconciler) dataDirMoved(ctx context.Context, pod *corev1.Pod, sts *appsv1.StatefulSet) (dataDirMove, bool) {
	to, toSays := specDataDir(&sts.Spec.Template.Spec)
	if _, podSays := specDataDir(&pod.Spec); !podSays && !toSays {
		return dataDirMove{Pod: pod.Name, From: DataDir, To: DataDir, Ready: isPodReady(pod)}, false
	}
	if !toSays {
		to = DataDir
	}
	record, hasRecord := sts.Annotations[DataDirAnnotation]
	from, why := r.podEngineDir(ctx, pod, record, hasRecord)
	m := dataDirMove{Pod: pod.Name, From: from, To: to, Why: why, Ready: isPodReady(pod)}
	return m, why != "" || from != to
}

// hasSyncedReplica reports whether c, replicated, has a replica that was
// Ready and in sync when last seen.
func hasSyncedReplica(c *kividbv1alpha1.KividbCluster) bool {
	if c.Spec.Replicas == 0 {
		return false
	}
	for _, p := range c.Status.Pods {
		if p.Role == kividbv1alpha1.RoleReplica && p.Ready && p.Synced {
			return true
		}
	}
	return false
}

// holdsDataDir reports whether c holds a `dir` change that would move a
// pod's files: sharded, or with no replica Ready and in sync to resync
// from. A replica added to go on is made from the template kept, syncs in
// the master's directory, and then the change goes ahead, replica first.
func holdsDataDir(c *kividbv1alpha1.KividbCluster) bool {
	return isSharded(c) || !hasSyncedReplica(c)
}

// stsTarget is a StatefulSet of a cluster, as it is to be.
type stsTarget struct {
	name     string
	selector map[string]string
	replicas int32
}

func targetOf(sts *appsv1.StatefulSet) stsTarget {
	t := stsTarget{name: sts.Name}
	if sts.Spec.Selector != nil {
		t.selector = sts.Spec.Selector.MatchLabels
	}
	if sts.Spec.Replicas != nil {
		t.replicas = *sts.Spec.Replicas
	}
	return t
}

// dataDirPlan is what a pass does about the `dir` of kdbConfig (#112).
type dataDirPlan struct {
	// moves: the pods a held change would move; none when not held.
	moves []dataDirMove
	// config is what the config file is rendered from: kdbConfig, or,
	// while held, kdbConfig with the `dir` the config file has now.
	config *kividbv1alpha1.KividbConfig
}

func (p *dataDirPlan) held() bool { return p != nil && len(p.moves) > 0 }

// movesOf is the moves of the pods of the StatefulSet named sts.
func (p *dataDirPlan) movesOf(sts string) []dataDirMove {
	var out []dataDirMove
	for _, m := range p.moves {
		if rest, ok := strings.CutPrefix(m.Pod, sts+"-"); ok {
			if _, err := strconv.Atoi(rest); err == nil {
				out = append(out, m)
			}
		}
	}
	return out
}

// planDataDir decides, before any of c's objects is written, whether a
// change of kdbConfig's `dir` is held (#112), for the StatefulSets of
// targets.
//
// A `dir` change is involved when a StatefulSet's template and config were
// last applied for another directory than kdbConfig's (DataDirAnnotation,
// or, without it, the config file's `dir`), or, for one that is not there,
// when the config file has another `dir` and its pods or volumes are
// there. Without one, nothing is held and no engine is asked: a change to
// any other directive never holds. With one, on a cluster that holds such
// a change (holdsDataDir), it is held if a pod would move: one whose files
// are elsewhere than kdbConfig's `dir`, or not known to be there, and one
// that is not there while its volume is, which would be made again from
// the new template.
func (r *KividbClusterReconciler) planDataDir(ctx context.Context, c *kividbv1alpha1.KividbCluster, kdbConfig *kividbv1alpha1.KividbConfig, targets []stsTarget) (*dataDirPlan, error) {
	plan := &dataDirPlan{config: kdbConfig}
	want := engineDataDir(kdbConfig)
	lines, stored, err := r.storedDirLines(ctx, c)
	if err != nil {
		return nil, err
	}
	storedDir := engineDataDir(dirLinesConfig(lines))

	type state struct {
		t         stsTarget
		sts       *appsv1.StatefulSet
		record    string
		hasRecord bool
		pods      []corev1.Pod
	}
	var states []state
	involved := false
	for _, t := range targets {
		st := state{t: t}
		var sts appsv1.StatefulSet
		switch err := r.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: t.name}, &sts); {
		case apierrors.IsNotFound(err):
		case err != nil:
			return nil, err
		default:
			st.sts = &sts
			st.record, st.hasRecord = sts.Annotations[DataDirAnnotation]
		}
		selector := t.selector
		if st.sts != nil && st.sts.Spec.Selector != nil && len(st.sts.Spec.Selector.MatchLabels) > 0 {
			selector = st.sts.Spec.Selector.MatchLabels
		}
		if len(selector) == 0 {
			return nil, fmt.Errorf("StatefulSet %s has no selector to find its pods by", t.name)
		}
		var pods corev1.PodList
		if err := r.List(ctx, &pods, client.InNamespace(c.Namespace), client.MatchingLabels(selector)); err != nil {
			return nil, fmt.Errorf("listing the pods of StatefulSet %s: %w", t.name, err)
		}
		st.pods = pods.Items
		switch {
		case st.hasRecord:
			involved = involved || st.record != want
		case stored && st.sts != nil:
			involved = involved || storedDir != want
		case stored && storedDir != want:
			// Not there: its pods, or its volumes, may be.
			if len(st.pods) > 0 {
				involved = true
			} else if has, err := r.anyVolume(ctx, c, t.name, t.replicas); err != nil {
				return nil, err
			} else if has {
				involved = true
			}
		}
		states = append(states, st)
	}
	if !involved || !holdsDataDir(c) {
		return plan, nil
	}

	for _, st := range states {
		seen := map[string]bool{}
		for i := range st.pods {
			p := &st.pods[i]
			seen[p.Name] = true
			from, why := r.podEngineDir(ctx, p, st.record, st.hasRecord)
			if why != "" || from != want {
				plan.moves = append(plan.moves, dataDirMove{Pod: p.Name, From: from, To: want, Why: why, Ready: isPodReady(p)})
			}
		}
		n := st.t.replicas
		if st.sts != nil && st.sts.Spec.Replicas != nil && *st.sts.Spec.Replicas > n {
			n = *st.sts.Spec.Replicas
		}
		for i := int32(0); i < n; i++ {
			name := fmt.Sprintf("%s-%d", st.t.name, i)
			if seen[name] {
				continue
			}
			if has, err := r.hasVolume(ctx, c, name); err != nil {
				return nil, err
			} else if has {
				plan.moves = append(plan.moves, dataDirMove{Pod: name, To: want, Why: whyNoPod})
			}
		}
	}
	if len(plan.moves) == 0 {
		return plan, nil
	}
	sortMoves(plan.moves)
	if stored {
		plan.config = withDirLines(kdbConfig, lines)
	}
	return plan, nil
}

// hasVolume reports whether pod's data volume (its PersistentVolumeClaim)
// is there.
func (r *KividbClusterReconciler) hasVolume(ctx context.Context, c *kividbv1alpha1.KividbCluster, pod string) (bool, error) {
	var pvc corev1.PersistentVolumeClaim
	switch err := r.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: "data-" + pod}, &pvc); {
	case apierrors.IsNotFound(err):
		return false, nil
	case err != nil:
		return false, err
	}
	return true, nil
}

// anyVolume reports whether any of the first n pods of the StatefulSet sts
// has its data volume.
func (r *KividbClusterReconciler) anyVolume(ctx context.Context, c *kividbv1alpha1.KividbCluster, sts string, n int32) (bool, error) {
	for i := int32(0); i < n; i++ {
		if has, err := r.hasVolume(ctx, c, fmt.Sprintf("%s-%d", sts, i)); err != nil || has {
			return has, err
		}
	}
	return false, nil
}

// storedDirLines is the `dir` lines of c's config file as it is stored (a
// spelling of dir to its value, as written), and whether it is there.
func (r *KividbClusterReconciler) storedDirLines(ctx context.Context, c *kividbv1alpha1.KividbCluster) (map[string]string, bool, error) {
	var s corev1.Secret
	switch err := r.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: configSecretName(c)}, &s); {
	case apierrors.IsNotFound(err):
		return nil, false, nil
	case err != nil:
		return nil, false, err
	}
	conf, ok := s.Data[ConfigFileName]
	if !ok {
		if v, ok := s.StringData[ConfigFileName]; ok {
			conf = []byte(v)
		}
	}
	return configDirLines(string(conf)), true, nil
}

// configDirLines is the `dir` lines of a rendered config file: each
// spelling of dir, to its value as written.
func configDirLines(conf string) map[string]string {
	lines := map[string]string{}
	for _, line := range strings.Split(conf, "\n") {
		k, v, _ := strings.Cut(line, " ")
		if strings.EqualFold(k, "dir") {
			lines[k] = v
		}
	}
	return lines
}

// dirLinesConfig is a KividbConfig of lines alone, for engineDataDir.
func dirLinesConfig(lines map[string]string) *kividbv1alpha1.KividbConfig {
	if len(lines) == 0 {
		return nil
	}
	return &kividbv1alpha1.KividbConfig{Spec: kividbv1alpha1.KividbConfigSpec{Directives: lines}}
}

// withDirLines is kdbConfig with its `dir` replaced by lines (none: no
// `dir`), everything else as it is.
func withDirLines(kdbConfig *kividbv1alpha1.KividbConfig, lines map[string]string) *kividbv1alpha1.KividbConfig {
	var out *kividbv1alpha1.KividbConfig
	if kdbConfig != nil {
		out = kdbConfig.DeepCopy()
	} else {
		out = &kividbv1alpha1.KividbConfig{}
	}
	d := map[string]string{}
	for k, v := range out.Spec.Directives {
		if !strings.EqualFold(k, "dir") {
			d[k] = v
		}
	}
	for k, v := range lines {
		d[k] = v
	}
	out.Spec.Directives = d
	return out
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
// where is not known (why)".
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
// pod templates, and the config file its `dir`, meanwhile.
//
// The way out is to set `dir` back, or, on a replicated cluster, to have a
// replica Ready and in sync: it is made from the template kept, syncs in
// the master's directory, and then the change goes ahead, replica first.
// That needs the master's engine running: one that is not Ready has to
// have `dir` set back. There is no way to have a held pod start empty on
// the new `dir`: deleting it brings it back in the directory its files are
// in. A pod whose files are not known to be anywhere (a pod made by
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
		if !m.Ready && m.Why != whyNoPod {
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
		fmt.Fprintf(&b, "with no replica Ready and in sync to resync from, %s would start empty, and scheduled backups would upload that. ", replacement)
		switch {
		case len(unready) == 1:
			fmt.Fprintf(&b, "Set dir back: %s is not Ready, so a replica would have nothing to sync from", unready[0])
		case len(unready) > 1:
			fmt.Fprintf(&b, "Set dir back: %s are not Ready, so a replica would have nothing to sync from", strings.Join(unready, ", "))
		case c.Spec.Replicas > 0:
			b.WriteString("The change goes ahead once a replica is Ready and in sync; or set dir back")
		default:
			b.WriteString("Add a replica (spec.replicas: 1): it starts in the master's directory, and once it is Ready and in sync the change goes ahead, replica first. Or set dir back")
		}
	}
	if len(unknown) > 0 {
		fmt.Fprintf(&b, ". The operator could not ask where the files of %s are: a pod made by operator 0.7.1 or earlier takes dir from its config file, and may run in one made by hand. See where with CONFIG GET dir as a user allowed CONFIG (TROUBLESHOOTING.md, \"Where a pod keeps its files\") and set dir to that, or allow the agent's user CONFIG GET; the operator asks again on its next passes",
			strings.Join(unknown, ", "))
	}
	if kept {
		b.WriteString(". Until then the StatefulSet keeps its current pod template and the config file its dir, so other changes to the pod template wait too, and a pod recreated meanwhile (a drain, an eviction, a KividbDbOps restart) starts in the directory its files are in")
	}
	return b.String()
}
