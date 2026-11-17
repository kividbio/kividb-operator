package controller

import (
	"context"
	"strings"
	"testing"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// A KividbConfig `dir` is where the engine keeps its snapshot and
// append-only file: it changes into it at start, from /data. One that stays
// on the pod's volume is passed through; one that leaves it is refused,
// naming the KividbConfig and the path it resolves to, in a sharded cluster
// as in a replicated one (#109). The value is read as the engine reads it:
// unquoted, up to a comment.
func TestKividbConfigDirMustStayOnTheVolume(t *testing.T) {
	cfg := func(dir string) *kividbv1alpha1.KividbConfig {
		return &kividbv1alpha1.KividbConfig{ObjectMeta: metav1.ObjectMeta{Name: "cfg"},
			Spec: kividbv1alpha1.KividbConfigSpec{Directives: map[string]string{"dir": dir}}}
	}
	for _, tc := range []struct {
		dir     string
		refused string // the path the error names; "" when passed through
	}{
		{"", ""},
		{"sub", ""},
		{"./sub/../other", ""},
		{"/data", ""},
		{"/data/", ""},
		{"/data/sub", ""},
		{"/data/sub # kept here", ""},
		{"/var/lib/kividb", "/var/lib/kividb"},
		{"/data/../tmp", "/tmp"},
		{"../tmp", "/tmp"},
		{"/data2", "/data2"},
		{`"/tmp"`, "/tmp"},
		{"'/tmp'", "/tmp"},
	} {
		for _, c := range []*kividbv1alpha1.KividbCluster{testCluster("r"), shardedCluster("s", 2, 0)} {
			err := validateKividbConfig(c, cfg(tc.dir))
			switch {
			case tc.refused == "" && err != nil:
				t.Errorf("%s: dir %q refused: %v", c.Name, tc.dir, err)
			case tc.refused != "" && err == nil:
				t.Errorf("%s: dir %q passed through, want it refused", c.Name, tc.dir)
			case tc.refused != "" && (!strings.Contains(err.Error(), "KividbConfig cfg") || !strings.Contains(err.Error(), tc.refused+",")):
				t.Errorf("%s: dir %q: %v, want it to name KividbConfig cfg and %s", c.Name, tc.dir, err, tc.refused)
			}
		}
	}
}

// A running cluster whose KividbConfig moves `dir` off the volume is not
// rolled onto it: its StatefulSet is left as it is, and Ready says why
// (InvalidSpec). Once `dir` is back on the volume, the held change goes out
// and the error is gone.
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
	var held appsv1.StatefulSet
	if err := f.cl.Get(ctx, key, &held); err != nil {
		t.Fatal(err)
	}
	if held.ResourceVersion != before.ResourceVersion || held.Generation != before.Generation ||
		!equality.Semantic.DeepEqual(held.Spec.Template, before.Spec.Template) {
		t.Errorf("StatefulSet updated while dir is off the volume (generation %d -> %d)", before.Generation, held.Generation)
	}
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
