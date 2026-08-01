package controller

import (
	"testing"

	"github.com/kividbio/kividb-operator/internal/agentapi"
	corev1 "k8s.io/api/core/v1"
)

func TestElectReplica(t *testing.T) {
	t.Parallel()

	readyPod := func(name string) *corev1.Pod {
		return &corev1.Pod{}
	}

	tests := []struct {
		name    string
		views   map[string]*podView
		exclude string
		want    string
	}{
		{
			name: "picks highest replication offset",
			views: map[string]*podView{
				"pod-a": {pod: readyPod("pod-a"), ready: true, status: &agentapi.StatusResponse{ReplicationOffset: 10}},
				"pod-b": {pod: readyPod("pod-b"), ready: true, status: &agentapi.StatusResponse{ReplicationOffset: 50}},
				"pod-c": {pod: readyPod("pod-c"), ready: true, status: &agentapi.StatusResponse{ReplicationOffset: 30}},
			},
			want: "pod-b",
		},
		{
			name: "excludes the named exclude pod",
			views: map[string]*podView{
				"master": {pod: readyPod("master"), ready: true, status: &agentapi.StatusResponse{ReplicationOffset: 100}},
				"pod-a":  {pod: readyPod("pod-a"), ready: true, status: &agentapi.StatusResponse{ReplicationOffset: 40}},
				"pod-b":  {pod: readyPod("pod-b"), ready: true, status: &agentapi.StatusResponse{ReplicationOffset: 80}},
			},
			exclude: "master",
			want:    "pod-b",
		},
		{
			name: "skips non-ready pods",
			views: map[string]*podView{
				"pod-a": {pod: readyPod("pod-a"), ready: false, status: &agentapi.StatusResponse{ReplicationOffset: 999}},
				"pod-b": {pod: readyPod("pod-b"), ready: true, status: &agentapi.StatusResponse{ReplicationOffset: 5}},
			},
			want: "pod-b",
		},
		{
			name: "tie-break by name lexicographically smaller wins",
			views: map[string]*podView{
				"pod-z": {pod: readyPod("pod-z"), ready: true, status: &agentapi.StatusResponse{ReplicationOffset: 10}},
				"pod-a": {pod: readyPod("pod-a"), ready: true, status: &agentapi.StatusResponse{ReplicationOffset: 10}},
				"pod-m": {pod: readyPod("pod-m"), ready: true, status: &agentapi.StatusResponse{ReplicationOffset: 10}},
			},
			want: "pod-a",
		},
		{
			name: "returns empty string when no candidates",
			views: map[string]*podView{
				"master": {pod: readyPod("master"), ready: true, status: &agentapi.StatusResponse{ReplicationOffset: 1}},
				"pod-a":  {pod: readyPod("pod-a"), ready: false, status: &agentapi.StatusResponse{ReplicationOffset: 2}},
			},
			exclude: "master",
			want:    "",
		},
		{
			name:  "returns empty string when views empty",
			views: map[string]*podView{},
			want:  "",
		},
		{
			name: "treats nil status as offset 0",
			views: map[string]*podView{
				"pod-a": {pod: readyPod("pod-a"), ready: true, status: nil},
				"pod-b": {pod: readyPod("pod-b"), ready: true, status: &agentapi.StatusResponse{ReplicationOffset: 1}},
			},
			want: "pod-b",
		},
		{
			name: "nil status ties with explicit zero offset, name wins",
			views: map[string]*podView{
				"pod-b": {pod: readyPod("pod-b"), ready: true, status: &agentapi.StatusResponse{ReplicationOffset: 0}},
				"pod-a": {pod: readyPod("pod-a"), ready: true, status: nil},
			},
			want: "pod-a",
		},
		{
			name: "all nil status: picks lexicographically smallest ready",
			views: map[string]*podView{
				"pod-c": {pod: readyPod("pod-c"), ready: true, status: nil},
				"pod-a": {pod: readyPod("pod-a"), ready: true, status: nil},
				"pod-b": {pod: readyPod("pod-b"), ready: false, status: nil},
			},
			want: "pod-a",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := electReplica(tt.views, tt.exclude)
			if got != tt.want {
				t.Fatalf("electReplica() = %q, want %q", got, tt.want)
			}
		})
	}
}
