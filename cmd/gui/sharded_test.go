package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func shardedFixture() *kividbv1alpha1.KividbCluster {
	shards := int32(2)
	zero, one := int32(0), int32(1)
	return &kividbv1alpha1.KividbCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "prod"},
		Spec: kividbv1alpha1.KividbClusterSpec{
			Shards: &shards, Replicas: 1,
			SnapshotConfigRef: &corev1.LocalObjectReference{Name: "hourly"},
		},
		Status: kividbv1alpha1.KividbClusterStatus{
			Phase: kividbv1alpha1.PhaseRebalancing,
			Pods: []kividbv1alpha1.KividbPodStatus{
				{Name: "cache-0-0", Role: kividbv1alpha1.RoleMaster, Ready: true, Shard: &zero, NodeID: "a"},
				{Name: "cache-0-1", Role: kividbv1alpha1.RoleReplica, Ready: true, Shard: &zero},
				{Name: "cache-1-0", Role: kividbv1alpha1.RoleReplica, Ready: true, Shard: &one},
				{Name: "cache-1-1", Role: kividbv1alpha1.RoleMaster, Ready: false, Shard: &one},
			},
			Shards: []kividbv1alpha1.ShardStatus{
				{Index: 0, MasterPod: "cache-0-0", Slots: []string{"0-8191"}},
				{Index: 1, MasterPod: "cache-1-1", Slots: []string{"8192-16383"}},
			},
			Cluster: &kividbv1alpha1.ClusterModeStatus{
				State: "ok", SlotsAssigned: 16384, Shards: 3,
				Rebalance: &kividbv1alpha1.RebalanceStatus{From: 3, To: 2, Phase: kividbv1alpha1.RebalanceMovingSlots},
			},
		},
	}
}

func guiServer(t *testing.T, objs []client.Object, kube ...runtime.Object) *server {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = kividbv1alpha1.AddToScheme(scheme)
	return &server{
		ctrlClient: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build(),
		clientset:  k8sfake.NewSimpleClientset(kube...),
	}
}

func svc(name, kind string) *corev1.Service {
	return &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "prod", Labels: map[string]string{
		kividbv1alpha1.ClusterLabel: "cache", serviceKindLabel: kind,
	}}, Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, Ports: []corev1.ServicePort{{Port: 6380}}}}
}

func sts(name string, ready int32) *appsv1.StatefulSet {
	two := int32(2)
	return &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "prod"}, Spec: appsv1.StatefulSetSpec{Replicas: &two}, Status: appsv1.StatefulSetStatus{ReadyReplicas: ready}}
}

func TestShardedClusterDetail(t *testing.T) {
	c := shardedFixture()
	s := guiServer(t, []client.Object{c},
		svc("cache", "cluster"), svc("cache-replicas", "replicas"), svc("cache-shard-0", "shard"), svc("cache-0-headless", "headless"), svc("cache-0-0", "pod"),
		sts("cache-0", 2), sts("cache-1", 1), sts("cache-2", 2),
	)
	sum := summarizeCluster(c)
	if sum.Shards != 2 || sum.DesiredPods != 6 || sum.SlotsAssigned != 16384 || sum.ClusterState != "ok" || !sum.Rebalancing {
		t.Fatalf("summary %+v", sum)
	}
	det, err := getClusterDetail(context.Background(), s.ctrlClient, s.clientset, "prod", "cache")
	if err != nil {
		t.Fatal(err)
	}
	if det.Cluster == nil || det.Cluster.Rebalance.From != 3 || len(det.ShardStatus) != 2 || det.ShardStatus[1].MasterPod != "cache-1-1" {
		t.Fatalf("cluster %+v shards %+v", det.Cluster, det.ShardStatus)
	}
	if det.Pods[2].Shard == nil || *det.Pods[2].Shard != 1 {
		t.Fatalf("pods %+v", det.Pods)
	}
	var kinds []string
	for _, v := range det.Services {
		kinds = append(kinds, v.Name+":"+v.Kind)
	}
	if strings.Join(kinds, " ") != "cache:cluster cache-replicas:replicas cache-shard-0:shard cache-0-headless:headless" {
		t.Fatalf("services %v", kinds)
	}
	// Three StatefulSets while shard 2 is being removed.
	if det.StatefulSet == nil || det.StatefulSet.DesiredReplicas != 6 || det.StatefulSet.ReadyReplicas != 5 {
		t.Fatalf("statefulsets %+v", det.StatefulSet)
	}
}

func scaleRequest(s *server, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/api/clusters/prod/cache/scale", strings.NewReader(body))
	req.SetPathValue("namespace", "prod")
	req.SetPathValue("name", "cache")
	rec := httptest.NewRecorder()
	s.handleAPIScale(rec, req)
	return rec
}

func TestScaleShards(t *testing.T) {
	c := shardedFixture()
	s := guiServer(t, []client.Object{c})
	if rec := scaleRequest(s, `{"shards":3}`); rec.Code != http.StatusOK {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
	var got kividbv1alpha1.KividbCluster
	_ = s.ctrlClient.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "cache"}, &got)
	if *got.Spec.Shards != 3 || got.Spec.Replicas != 1 {
		t.Fatalf("spec %d shards, %d replicas", *got.Spec.Shards, got.Spec.Replicas)
	}
	for _, body := range []string{`{"shards":0}`, `{"shards":13}`, `{"shards":2,"replicas":1}`, `{}`} {
		if rec := scaleRequest(s, body); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: %d %s", body, rec.Code, rec.Body)
		}
	}
	// 3 shards × 9 pods each = 27 nodes: refused.
	if rec := scaleRequest(s, `{"replicas":8}`); rec.Code != http.StatusBadRequest {
		t.Errorf("27 nodes: %d", rec.Code)
	}

	plain := shardedFixture()
	plain.Name, plain.Spec.Shards = "plain", nil
	s = guiServer(t, []client.Object{plain})
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"shards":2}`))
	req.SetPathValue("namespace", "prod")
	req.SetPathValue("name", "plain")
	rec := httptest.NewRecorder()
	s.handleAPIScale(rec, req)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "not sharded") {
		t.Fatalf("shards on an unsharded cluster: %d %s", rec.Code, rec.Body)
	}
}

func TestShardedSnapshotStartsAJobFromTheCronJob(t *testing.T) {
	c := shardedFixture()
	cj := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{Name: "cache-backup", Namespace: "prod", UID: "cj-uid"},
		Spec: batchv1.CronJobSpec{JobTemplate: batchv1.JobTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{kividbv1alpha1.ClusterLabel: "cache", "kividb.io/component": "backup"}},
			Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "backup-trigger", Args: []string{"backup-trigger", "--shards", "x"}}},
			}}},
		}},
	}
	s := guiServer(t, []client.Object{c}, cj)
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	req.SetPathValue("namespace", "prod")
	req.SetPathValue("name", "cache")
	rec := httptest.NewRecorder()
	s.handleAPISnapshot(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
	var out map[string]string
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	job, err := s.clientset.BatchV1().Jobs("prod").Get(context.Background(), out["job"], metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(job.Name, "cache-backup-m") || len(job.Name) > 63 || job.Labels["kividb.io/component"] != "backup" ||
		job.OwnerReferences[0].UID != "cj-uid" || job.Spec.Template.Spec.Containers[0].Args[1] != "--shards" {
		t.Fatalf("job %+v", job)
	}
}
