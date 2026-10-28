package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Promoting a replica of a replicated cluster asks the operator for a
// switchover, which loses nothing; it no longer promotes the pod itself.
func TestPromoteAsksTheOperatorForASwitchover(t *testing.T) {
	c := &kividbv1alpha1.KividbCluster{ObjectMeta: metav1.ObjectMeta{Name: "rep", Namespace: "prod"}, Spec: kividbv1alpha1.KividbClusterSpec{Replicas: 1}}
	pod := func(name, role string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "prod", Labels: map[string]string{
				kividbv1alpha1.ClusterLabel: "rep", appNameLabel: appNameValue, kividbv1alpha1.RoleLabel: role,
			}},
			Status: corev1.PodStatus{PodIP: "10.0.0." + name[len(name)-1:]},
		}
	}
	s := guiServer(t, []client.Object{c}, pod("rep-0", "master"), pod("rep-1", "replica"))
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"pod":"rep-1"}`))
	req.SetPathValue("namespace", "prod")
	req.SetPathValue("name", "rep")
	rec := httptest.NewRecorder()
	s.handleAPIPromote(rec, req)
	if rec.Code != http.StatusAccepted || !strings.Contains(rec.Body.String(), `"requested":"rep-1"`) {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
	var got kividbv1alpha1.KividbCluster
	_ = s.ctrlClient.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "rep"}, &got)
	if got.Annotations[switchoverToAnnotation] != "rep-1" {
		t.Fatalf("annotations %v", got.Annotations)
	}
}
