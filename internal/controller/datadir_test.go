package controller

import (
	"context"
	"strings"
	"testing"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func directivesConfig(d map[string]string) *kividbv1alpha1.KividbConfig {
	return &kividbv1alpha1.KividbConfig{ObjectMeta: metav1.ObjectMeta{Name: "cfg"},
		Spec: kividbv1alpha1.KividbConfigSpec{Directives: d}}
}

// A KividbConfig `dir` is where the engine keeps its snapshot and
// append-only file: it changes into it at start, from /data. One that stays
// on the pod's volume is passed through; one that leaves it is refused,
// naming the KividbConfig and the path it resolves to, in a sharded cluster
// as in a replicated one (#109). The value is read as the engine reads it
// (split_config_line): "..." with its backslash escapes, '...' literal, up
// to a comment; one the engine could not read, or more or fewer than one
// word, is refused too, since the engine would not start on it.
func TestKividbConfigDirMustStayOnTheVolume(t *testing.T) {
	for _, tc := range []struct {
		dir  string
		want string // in the error; "" when passed through
	}{
		{`sub`, ""},
		{`./sub/../other`, ""},
		{`/data`, ""},
		{`/data/`, ""},
		{`/data/sub`, ""},
		{`/data/sub # kept here`, ""},
		{`"/data/sub"`, ""},
		{`""`, ""},
		{`'/data/\..'`, ""},
		{`/var/lib/kividb`, "which is /var/lib/kividb,"},
		{`/data/../tmp`, "which is /tmp,"},
		{`../tmp`, "which is /tmp,"},
		{`/data2`, "which is /data2,"},
		{`"/tmp"`, "which is /tmp,"},
		{`'/tmp'`, "which is /tmp,"},
		{`"/data/\../tmp"`, "which is /tmp,"},
		{`"/data/.\./x"`, "which is /x,"},
		{`"\/tmp"`, "which is /tmp,"},
		{`"/tmp`, "cannot read: unbalanced quotes"},
		{`"/data"x`, "cannot read: a closing quote must be followed by a space"},
		{`/data /tmp`, "one value there, not 2"},
		{``, "one value there, not 0"},
	} {
		for _, c := range []*kividbv1alpha1.KividbCluster{testCluster("r"), shardedCluster("s", 2, 0)} {
			err := validateKividbConfig(c, directivesConfig(map[string]string{"dir": tc.dir}))
			switch {
			case tc.want == "" && err != nil:
				t.Errorf("%s: dir %s refused: %v", c.Name, tc.dir, err)
			case tc.want != "" && err == nil:
				t.Errorf("%s: dir %s passed through, want it refused (%s)", c.Name, tc.dir, tc.want)
			case tc.want != "" && (!strings.Contains(err.Error(), "KividbConfig cfg") || !strings.Contains(err.Error(), tc.want)):
				t.Errorf("%s: dir %s: %v, want it to name KividbConfig cfg and say %q", c.Name, tc.dir, err, tc.want)
			}
		}
	}
}

// Each directive is written as one line, "name value", and the engine reads
// the line back word by word: a name that is not one plain word, or a value
// with a line break, would give the engine another directive than the one
// checked, for every directive and not only `dir`. Directive names are
// matched as the engine matches them, whatever their case.
func TestKividbConfigDirectivesAreOneLineEach(t *testing.T) {
	for _, tc := range []struct {
		name       string
		directives map[string]string
		want       string // in the error; "" when accepted
	}{
		{"a second line in a value", map[string]string{"dir": "/data\ndir /tmp"}, "a value with a line break"},
		{"a carriage return in a value", map[string]string{"loglevel": "verbose\r"}, "a value with a line break"},
		{"a name with a leading space", map[string]string{" dir": "/tmp"}, `named " dir"`},
		{"a name with a tab", map[string]string{"dir\t": "/tmp"}, `named "dir\t"`},
		{"a name with a line break", map[string]string{"loglevel\ndir": "/tmp"}, `named "loglevel\ndir"`},
		{"an empty name", map[string]string{"": "dir /tmp"}, `named ""`},
		{"a quoted name", map[string]string{`"dir"`: "/tmp"}, `named "\"dir\""`},
		{"a name that starts a comment", map[string]string{"#dir": "/tmp"}, `named "#dir"`},
		{"DIR off the volume", map[string]string{"DIR": "/tmp"}, `sets DIR "/tmp", which is /tmp,`},
		{"dir on it, DIR off it", map[string]string{"dir": "sub", "DIR": "/tmp"}, `sets DIR "/tmp"`},
		{"plain directives", map[string]string{"loglevel": "verbose", "dir": "sub", "maxmemory": "1gb"}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateKividbConfig(testCluster("r"), directivesConfig(tc.directives))
			switch {
			case tc.want == "" && err != nil:
				t.Fatalf("refused: %v", err)
			case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
				t.Fatalf("error %v, want it to say %q", err, tc.want)
			}
		})
	}
}

// Two directives off the volume: the message names the same one every
// time, the first by name, not whichever a map gives first.
func TestKividbConfigDirMessageIsTheSameEveryTime(t *testing.T) {
	cfg := directivesConfig(map[string]string{"dir": "/b", "DIR": "/a"})
	first := validateKividbConfig(testCluster("r"), cfg)
	if first == nil || !strings.Contains(first.Error(), `sets DIR "/a"`) {
		t.Fatalf("error %v, want it to name DIR", first)
	}
	for i := 0; i < 50; i++ {
		if err := validateKividbConfig(testCluster("r"), cfg); err == nil || err.Error() != first.Error() {
			t.Fatalf("error %v, then %v", first, err)
		}
	}
}

// clusterObjects is the resource version of every object of a kind the
// operator writes for a cluster, by kind and name.
func clusterObjects(t *testing.T, cl client.Client) map[string]string {
	t.Helper()
	out := map[string]string{}
	for kind, list := range map[string]client.ObjectList{
		"StatefulSet":         &appsv1.StatefulSetList{},
		"Secret":              &corev1.SecretList{},
		"ConfigMap":           &corev1.ConfigMapList{},
		"Service":             &corev1.ServiceList{},
		"PodDisruptionBudget": &policyv1.PodDisruptionBudgetList{},
		"CronJob":             &batchv1.CronJobList{},
	} {
		if err := cl.List(context.Background(), list, client.InNamespace("default")); err != nil {
			t.Fatal(err)
		}
		items, err := meta.ExtractList(list)
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range items {
			obj := o.(client.Object)
			out[kind+"/"+obj.GetName()] = obj.GetResourceVersion()
		}
	}
	return out
}

func sameObjects(t *testing.T, what string, before, after map[string]string) {
	t.Helper()
	for k, v := range before {
		if after[k] != v {
			t.Errorf("%s: %s changed (resource version %s -> %q)", what, k, v, after[k])
		}
	}
	for k := range after {
		if _, ok := before[k]; !ok {
			t.Errorf("%s: %s created", what, k)
		}
	}
}

// A running cluster whose KividbConfig moves `dir` off the volume is not
// rolled onto it: none of its objects change (StatefulSet, the config
// Secret, Services, PDB), and Ready says why (InvalidSpec). Once `dir` is
// back on the volume, the held change goes out and the error is gone.
func TestReconcile_RunningClusterIsNotRolledOntoADirOffTheVolume(t *testing.T) {
	ctx := context.Background()
	conf := &kividbv1alpha1.KividbConfig{ObjectMeta: metav1.ObjectMeta{Name: "conf", Namespace: "default"}}
	f := newFailoverCase(t, func(c *kividbv1alpha1.KividbCluster) {
		c.Spec.ConfigRef = &corev1.LocalObjectReference{Name: "conf"}
	}, nil, conf)
	f.reconcile(t, reconcileInterval)
	key := client.ObjectKey{Namespace: "default", Name: statefulSetName(f.c)}
	var before appsv1.StatefulSet
	if err := f.cl.Get(ctx, key, &before); err != nil {
		t.Fatalf("StatefulSet not applied: %v", err)
	}
	objects := clusterObjects(t, f.cl)

	setDir := func(dir string) {
		t.Helper()
		if err := f.cl.Get(ctx, client.ObjectKeyFromObject(conf), conf); err != nil {
			t.Fatal(err)
		}
		conf.Spec.Directives = map[string]string{"dir": dir}
		if err := f.cl.Update(ctx, conf); err != nil {
			t.Fatal(err)
		}
	}
	setDir("/data/../var/lib/kividb")
	for i := 0; i < 2; i++ {
		f.reconcile(t, reconcileInterval)
	}
	sameObjects(t, "while dir is off the volume", objects, clusterObjects(t, f.cl))
	if ready := readyOf(t, f.cl, client.ObjectKeyFromObject(f.c)); ready == nil || ready.Status != metav1.ConditionFalse ||
		ready.Reason != "InvalidSpec" || !strings.Contains(ready.Message, "KividbConfig conf") || !strings.Contains(ready.Message, "/var/lib/kividb") {
		t.Errorf("Ready condition %+v, want False, InvalidSpec, naming KividbConfig conf and /var/lib/kividb", ready)
	}

	setDir("kividb")
	f.reconcile(t, reconcileInterval)
	var after appsv1.StatefulSet
	if err := f.cl.Get(ctx, key, &after); err != nil {
		t.Fatal(err)
	}
	if after.Spec.Template.Annotations[ConfigHashAnnotation] == before.Spec.Template.Annotations[ConfigHashAnnotation] {
		t.Error("the held directive change was not applied once dir was back on the volume")
	}
	if ready := readyOf(t, f.cl, client.ObjectKeyFromObject(f.c)); ready != nil && ready.Reason == "InvalidSpec" {
		t.Errorf("Ready condition %+v, want the error gone", ready)
	}
}

// A sharded cluster is held the same way: none of its shards' objects
// change, and its roles are still kept (reconcileFormed).
func TestReconcileSharded_RunningClusterIsNotRolledOntoADirOffTheVolume(t *testing.T) {
	ctx := context.Background()
	conf := &kividbv1alpha1.KividbConfig{ObjectMeta: metav1.ObjectMeta{Name: "conf", Namespace: "default"}}
	h := formedForReconcile(t, nil, conf)
	_ = h.r.Get(ctx, client.ObjectKeyFromObject(h.c), h.c)
	h.c.Spec.ConfigRef = &corev1.LocalObjectReference{Name: "conf"}
	if err := h.r.Update(ctx, h.c); err != nil {
		t.Fatal(err)
	}
	if _, err := h.r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(h.c)}); err != nil {
		t.Fatal(err)
	}
	objects := clusterObjects(t, h.r.Client)
	if objects["StatefulSet/"+shardStatefulSetName(h.c, 0)] == "" {
		t.Fatalf("shard 0's StatefulSet not applied: %v", objects)
	}

	if err := h.r.Get(ctx, client.ObjectKeyFromObject(conf), conf); err != nil {
		t.Fatal(err)
	}
	conf.Spec.Directives = map[string]string{"dir": "/var/lib/kividb"}
	if err := h.r.Update(ctx, conf); err != nil {
		t.Fatal(err)
	}
	reconcileFormed(t, h, "InvalidSpec", `KividbConfig conf sets dir "/var/lib/kividb", which is /var/lib/kividb,`)
	sameObjects(t, "while dir is off the volume", objects, clusterObjects(t, h.r.Client))
}

// A new cluster whose KividbConfig has a `dir` off the volume gets none of
// its objects: the engine would start with its files off the volume.
func TestReconcile_NewClusterWithADirOffTheVolumeCreatesNothing(t *testing.T) {
	conf := &kividbv1alpha1.KividbConfig{ObjectMeta: metav1.ObjectMeta{Name: "conf", Namespace: "default"},
		Spec: kividbv1alpha1.KividbConfigSpec{Directives: map[string]string{"dir": "/tmp"}}}
	f := newFailoverCase(t, func(c *kividbv1alpha1.KividbCluster) {
		c.Spec.ConfigRef = &corev1.LocalObjectReference{Name: "conf"}
	}, nil, conf)
	objects := clusterObjects(t, f.cl)
	for i := 0; i < 2; i++ {
		f.reconcile(t, reconcileInterval)
	}
	sameObjects(t, "a new cluster", objects, clusterObjects(t, f.cl))
	if ready := readyOf(t, f.cl, client.ObjectKeyFromObject(f.c)); ready == nil || ready.Reason != "InvalidSpec" ||
		!strings.Contains(ready.Message, `KividbConfig conf sets dir "/tmp"`) {
		t.Errorf("Ready condition %+v, want InvalidSpec naming KividbConfig conf and /tmp", ready)
	}
}
