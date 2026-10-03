package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	"github.com/kividbio/kividb-operator/internal/agentapi"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const agentHTTPTimeout = 10 * time.Second

func (s *server) requireNS(w http.ResponseWriter, namespace string) bool {
	if s.watchNamespace != "" && namespace != s.watchNamespace {
		writeJSONError(w, http.StatusForbidden, fmt.Errorf("namespace %q is not watched", namespace))
		return false
	}
	return true
}

func (s *server) handleAPIExec(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")
	name := r.PathValue("name")
	if !s.requireNS(w, namespace) {
		return
	}
	var body struct {
		Pod  string   `json:"pod"`
		Args []string `json:"args"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, err)
		return
	}
	if body.Pod == "" || len(body.Args) == 0 {
		writeJSONError(w, http.StatusBadRequest, fmt.Errorf("pod and args are required"))
		return
	}
	pod, err := s.clientset.CoreV1().Pods(namespace).Get(r.Context(), body.Pod, metav1.GetOptions{})
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err)
		return
	}
	if !isClusterPod(pod, name) {
		writeJSONError(w, http.StatusBadRequest, fmt.Errorf("pod %s is not part of cluster %s", body.Pod, name))
		return
	}
	if pod.Status.PodIP == "" {
		writeJSONError(w, http.StatusBadRequest, fmt.Errorf("pod has no IP yet"))
		return
	}
	out, err := postAgentJSON(r.Context(), pod.Status.PodIP, "/exec", agentapi.ExecRequest{Args: body.Args})
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(out)
}

func (s *server) handleAPIPodLogs(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")
	name := r.PathValue("name")
	podName := r.PathValue("pod")
	if !s.requireNS(w, namespace) {
		return
	}
	container := r.URL.Query().Get("container")
	if container == "" {
		container = "kividb"
	}
	tail := int64(200)
	if t := r.URL.Query().Get("tail"); t != "" {
		if n, err := strconv.ParseInt(t, 10, 64); err == nil && n > 0 && n <= 5000 {
			tail = n
		}
	}
	pod, err := s.clientset.CoreV1().Pods(namespace).Get(r.Context(), podName, metav1.GetOptions{})
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err)
		return
	}
	if !isClusterPod(pod, name) {
		writeJSONError(w, http.StatusBadRequest, fmt.Errorf("pod mismatch"))
		return
	}
	opts := &corev1.PodLogOptions{Container: container, TailLines: &tail}
	stream, err := s.clientset.CoreV1().Pods(namespace).GetLogs(podName, opts).Stream(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, err)
		return
	}
	defer stream.Close()
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.Copy(w, stream)
}

func (s *server) handleAPICreateRestart(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")
	name := r.PathValue("name")
	if !s.requireNS(w, namespace) {
		return
	}
	opName := fmt.Sprintf("%s-restart-%d", name, time.Now().Unix())
	op := &kividbv1alpha1.KividbDbOps{
		ObjectMeta: metav1.ObjectMeta{
			Name:      opName,
			Namespace: namespace,
			Labels: map[string]string{
				"kividb.io/cluster":            name,
				"app.kubernetes.io/managed-by": "kividb-operator-gui",
			},
		},
		Spec: kividbv1alpha1.KividbDbOpsSpec{
			ClusterRef: corev1.LocalObjectReference{Name: name},
			Op:         kividbv1alpha1.DbOpsRestart,
			Restart:    &kividbv1alpha1.DbOpsRestartSpec{Method: kividbv1alpha1.RestartInPlace},
		},
	}
	if err := s.ctrlClient.Create(r.Context(), op); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"name": opName, "namespace": namespace})
}

func (s *server) handleAPIListDbOps(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")
	name := r.PathValue("name")
	if !s.requireNS(w, namespace) {
		return
	}
	var list kividbv1alpha1.KividbDbOpsList
	if err := s.ctrlClient.List(r.Context(), &list, client.InNamespace(namespace), client.MatchingLabels{"kividb.io/cluster": name}); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, list.Items)
}

func (s *server) handleAPILiveStatus(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")
	name := r.PathValue("name")
	if !s.requireNS(w, namespace) {
		return
	}
	pods, err := s.clientset.CoreV1().Pods(namespace).List(r.Context(), metav1.ListOptions{
		LabelSelector: clusterLabelSelector(name),
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	type row struct {
		Pod               string `json:"pod"`
		Role              string `json:"role"`
		Ready             bool   `json:"ready"`
		IP                string `json:"ip"`
		ReplicationOffset int64  `json:"replicationOffset"`
		MasterHost        string `json:"masterHost,omitempty"`
		UsedMemory        string `json:"usedMemory,omitempty"`
		Error             string `json:"error,omitempty"`
	}
	out := make([]row, 0, len(pods.Items))
	for i := range pods.Items {
		p := &pods.Items[i]
		rr := row{
			Pod:   p.Name,
			Role:  p.Labels["kividb.io/role"],
			Ready: false,
			IP:    p.Status.PodIP,
		}
		for _, c := range p.Status.Conditions {
			if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
				rr.Ready = true
			}
		}
		if p.Status.PodIP != "" {
			if st, err := getAgentStatus(r.Context(), p.Status.PodIP); err != nil {
				rr.Error = err.Error()
			} else {
				rr.Role = string(st.Role)
				rr.ReplicationOffset = st.ReplicationOffset
				rr.MasterHost = st.MasterHost
			}
			if mem, err := scrapeUsedMemory(r.Context(), p.Status.PodIP); err == nil {
				rr.UsedMemory = mem
			}
		}
		out = append(out, rr)
	}
	writeJSON(w, http.StatusOK, out)
}

func postAgentJSON(ctx context.Context, podIP, path string, payload any) ([]byte, error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://%s:8081%s", podIP, path), bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: agentHTTPTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("agent %s: %s", path, string(body))
	}
	return body, nil
}

func getAgentStatus(ctx context.Context, podIP string) (*agentapi.StatusResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s:8081/status", podIP), nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: agentHTTPTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s", string(body))
	}
	var st agentapi.StatusResponse
	if err := json.Unmarshal(body, &st); err != nil {
		return nil, err
	}
	return &st, nil
}

func scrapeUsedMemory(ctx context.Context, podIP string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s:8081/metrics", podIP), nil)
	if err != nil {
		return "", err
	}
	client := &http.Client{Timeout: agentHTTPTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	// crude parse: look for kividb_used_memory_bytes <n>
	const prefix = "kividb_used_memory_bytes "
	for _, line := range bytes.Split(body, []byte("\n")) {
		if bytes.HasPrefix(line, []byte(prefix)) {
			return string(bytes.TrimSpace(bytes.TrimPrefix(line, []byte(prefix)))), nil
		}
	}
	return "", fmt.Errorf("metric not found")
}

// ensure cluster exists helper for ops that need it
func (s *server) getCluster(ctx context.Context, namespace, name string) (*kividbv1alpha1.KividbCluster, error) {
	var c kividbv1alpha1.KividbCluster
	if err := s.ctrlClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

func (s *server) handleAPIScale(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")
	name := r.PathValue("name")
	if !s.requireNS(w, namespace) {
		return
	}
	var body struct {
		Replicas *int32 `json:"replicas"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, err)
		return
	}
	if body.Replicas == nil || *body.Replicas < 0 {
		writeJSONError(w, http.StatusBadRequest, fmt.Errorf("replicas must be >= 0 (count of replica pods, not including master)"))
		return
	}
	c, err := s.getCluster(r.Context(), namespace, name)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, err)
		return
	}
	patch := client.MergeFrom(c.DeepCopy())
	c.Spec.Replicas = *body.Replicas
	if err := s.ctrlClient.Patch(r.Context(), c, patch); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"replicas": c.Spec.Replicas, "desiredPods": c.Spec.Replicas + 1})
}

func (s *server) handleAPIDeleteCluster(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")
	name := r.PathValue("name")
	if !s.requireNS(w, namespace) {
		return
	}
	c, err := s.getCluster(r.Context(), namespace, name)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, err)
		return
	}
	if err := s.ctrlClient.Delete(r.Context(), c); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"deleted": name})
}

func (s *server) handleAPIRestartPod(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")
	name := r.PathValue("name")
	podName := r.PathValue("pod")
	if !s.requireNS(w, namespace) {
		return
	}
	pod, err := s.clientset.CoreV1().Pods(namespace).Get(r.Context(), podName, metav1.GetOptions{})
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err)
		return
	}
	if !isClusterPod(pod, name) {
		writeJSONError(w, http.StatusBadRequest, fmt.Errorf("pod %s is not part of cluster %s", podName, name))
		return
	}
	grace := int64(10)
	if err := s.clientset.CoreV1().Pods(namespace).Delete(r.Context(), podName, metav1.DeleteOptions{GracePeriodSeconds: &grace}); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"restarted": podName})
}

func (s *server) handleAPIPromote(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")
	name := r.PathValue("name")
	if !s.requireNS(w, namespace) {
		return
	}
	var body struct {
		Pod string `json:"pod"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, err)
		return
	}
	if body.Pod == "" {
		writeJSONError(w, http.StatusBadRequest, fmt.Errorf("pod is required"))
		return
	}
	c, err := s.getCluster(r.Context(), namespace, name)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, err)
		return
	}
	pods, err := s.clientset.CoreV1().Pods(namespace).List(r.Context(), metav1.ListOptions{
		LabelSelector: clusterLabelSelector(name),
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	var target *corev1.Pod
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Name == body.Pod {
			target = p
			break
		}
	}
	if target == nil {
		writeJSONError(w, http.StatusBadRequest, fmt.Errorf("pod %s not found", body.Pod))
		return
	}
	if target.Status.PodIP == "" {
		writeJSONError(w, http.StatusBadRequest, fmt.Errorf("pod has no IP yet"))
		return
	}
	if target.Labels["kividb.io/role"] == "master" {
		writeJSONError(w, http.StatusBadRequest, fmt.Errorf("%s is already master", body.Pod))
		return
	}
	// A switchover is two steps here and the operator does the rest: make
	// the target a master, then turn the current master into its replica.
	// The operator sees its labeled master replicating from another healthy
	// master, adopts that pod (moving the role label, and with it the
	// master Service) and re-points the remaining replicas. Doing only
	// this much keeps a half-finished switchover recoverable: if the
	// second step fails the operator still has a healthy labeled master
	// and simply turns the target back into its replica.
	var current *corev1.Pod
	for i := range pods.Items {
		if pods.Items[i].Labels["kividb.io/role"] == "master" {
			current = &pods.Items[i]
			break
		}
	}
	if current == nil || current.Status.PodIP == "" {
		writeJSONError(w, http.StatusConflict, fmt.Errorf("cluster %s has no current master to switch over from; the operator will elect one", name))
		return
	}
	if _, err := postAgentJSON(r.Context(), target.Status.PodIP, "/promote", nil); err != nil {
		writeJSONError(w, http.StatusBadGateway, err)
		return
	}
	if _, err := postAgentJSON(r.Context(), current.Status.PodIP, "/replicaof", agentapi.ReplicaOfRequest{
		Host: target.Status.PodIP,
		Port: portOrDefault(c.Spec.Port),
	}); err != nil {
		writeJSONError(w, http.StatusBadGateway, fmt.Errorf("pointing %s at new master: %w", current.Name, err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"promoted": body.Pod})
}

func (s *server) handleAPISnapshot(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")
	name := r.PathValue("name")
	if !s.requireNS(w, namespace) {
		return
	}
	c, err := s.getCluster(r.Context(), namespace, name)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, err)
		return
	}
	if c.Spec.SnapshotConfigRef == nil || c.Spec.SnapshotConfigRef.Name == "" {
		writeJSONError(w, http.StatusBadRequest, fmt.Errorf("cluster has no snapshotConfigRef — configure backups first"))
		return
	}
	pods, err := s.clientset.CoreV1().Pods(namespace).List(r.Context(), metav1.ListOptions{
		LabelSelector: clusterLabelSelector(name) + ",kividb.io/role=master",
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	if len(pods.Items) == 0 || pods.Items[0].Status.PodIP == "" {
		writeJSONError(w, http.StatusBadRequest, fmt.Errorf("no ready master pod to snapshot"))
		return
	}
	out, err := postAgentJSONTimeout(r.Context(), pods.Items[0].Status.PodIP, "/backup", nil, 5*time.Minute)
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(out)
}

func postAgentJSONTimeout(ctx context.Context, podIP, path string, payload any, timeout time.Duration) ([]byte, error) {
	var body io.Reader
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://%s:8081%s", podIP, path), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("agent %s: %s", path, string(respBody))
	}
	return respBody, nil
}
