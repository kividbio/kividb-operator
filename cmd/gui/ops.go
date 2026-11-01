package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	"github.com/kividbio/kividb-operator/internal/agentapi"
	batchv1 "k8s.io/api/batch/v1"
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
	// By the DbOps' own cluster reference: one created with kubectl
	// carries no kividb.io/cluster label (the GUI's own do).
	var list kividbv1alpha1.KividbDbOpsList
	if err := s.ctrlClient.List(r.Context(), &list, client.InNamespace(namespace)); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	ops := make([]kividbv1alpha1.KividbDbOps, 0, len(list.Items))
	for _, op := range list.Items {
		if op.Spec.ClusterRef.Name == name {
			ops = append(ops, op)
		}
	}
	writeJSON(w, http.StatusOK, ops)
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
		// MasterLinkUp: a replica's link to its master is up.
		MasterLinkUp *bool  `json:"masterLinkUp,omitempty"`
		Keys         *int64 `json:"keys,omitempty"`
		AofEnabled   bool   `json:"aofEnabled"`
		UsedMemory   string `json:"usedMemory,omitempty"`
		Error        string `json:"error,omitempty"`
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
				rr.MasterLinkUp = st.MasterLinkUp
				rr.AofEnabled = st.AofEnabled
				if st.KeyCountKnown {
					keys := st.KeyCount
					rr.Keys = &keys
				}
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
	agentapi.SetToken(req, agentToken())
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
	agentapi.SetToken(req, agentToken())
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
		Shards   *int32 `json:"shards"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, err)
		return
	}
	if (body.Replicas == nil) == (body.Shards == nil) {
		writeJSONError(w, http.StatusBadRequest, fmt.Errorf("set one of replicas or shards"))
		return
	}
	if body.Replicas != nil && *body.Replicas < 0 {
		writeJSONError(w, http.StatusBadRequest, fmt.Errorf("replicas must be >= 0 (count of replica pods, not including master)"))
		return
	}
	c, err := s.getCluster(r.Context(), namespace, name)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, err)
		return
	}
	patch := client.MergeFrom(c.DeepCopy())
	if body.Replicas != nil {
		c.Spec.Replicas = *body.Replicas
	}
	if body.Shards != nil {
		if c.Spec.Shards == nil {
			writeJSONError(w, http.StatusBadRequest, fmt.Errorf("%s is not sharded; a cluster is sharded or not from its creation", name))
			return
		}
		if *body.Shards < 1 {
			writeJSONError(w, http.StatusBadRequest, fmt.Errorf("a sharded cluster needs at least 1 shard"))
			return
		}
		c.Spec.Shards = body.Shards
	}
	if c.Spec.Shards != nil && *c.Spec.Shards*(c.Spec.Replicas+1) > maxShardedNodes {
		writeJSONError(w, http.StatusBadRequest, fmt.Errorf("%d shards × %d pods each is more than the %d nodes supported", *c.Spec.Shards, c.Spec.Replicas+1, maxShardedNodes))
		return
	}
	if err := s.ctrlClient.Patch(r.Context(), c, patch); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"replicas": c.Spec.Replicas, "shards": specShards(c), "desiredPods": desiredPods(c)})
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
	// The pod's own grace period: a master's preStop hook uses part of it
	// to hand the master role over, and kividb the rest to save.
	if err := s.clientset.CoreV1().Pods(namespace).Delete(r.Context(), podName, metav1.DeleteOptions{}); err != nil {
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
	if c.Spec.Shards != nil {
		// The engine fails over within the shard: CLUSTER FAILOVER on the
		// replica, which its master hands over to without losing a write.
		// Its OK only means the failover has started: from engine 1.2 the
		// replica gives it up when it has not caught up and won its
		// election within 5 s, and the master carries on. So this reports
		// a request, and the view shows whether the role moved.
		//
		// First noted on the cluster, so the operator reports the move as
		// asked for, not as a failover (it may see it within 100 ms).
		patch := client.MergeFrom(c.DeepCopy())
		if c.Annotations == nil {
			c.Annotations = map[string]string{}
		}
		c.Annotations[kividbv1alpha1.ShardPromoteAnnotation] = body.Pod + " " + time.Now().UTC().Format(time.RFC3339)
		if err := s.ctrlClient.Patch(r.Context(), c, patch); err != nil {
			log.Printf("noting the promote of %s on %s/%s: %v", body.Pod, namespace, name, err)
		}
		if _, err := s.postAgent(r.Context(), target.Status.PodIP, "/cluster/failover", nil); err != nil {
			writeJSONError(w, http.StatusBadGateway, err)
			return
		}
		// The operator reports the handovers it asks for itself; this one
		// is the GUI's, so the GUI says so (the operator's ShardSwitchover).
		shard, master := target.Labels[shardLabel], ""
		for _, st := range c.Status.Shards {
			if strconv.Itoa(int(st.Index)) == shard {
				master = st.MasterPod
			}
		}
		if master == "" {
			master = "its master"
		}
		s.clusterEvent(r.Context(), c, corev1.EventTypeNormal, "ShardSwitchover",
			fmt.Sprintf("shard %s: asked %s to take over from %s (CLUSTER FAILOVER), from the GUI", shard, body.Pod, master))
		writeJSON(w, http.StatusAccepted, map[string]string{"requested": body.Pod})
		return
	}
	// The operator does the switchover (it holds the master's writes until
	// the target has applied every one of them, then promotes it); this
	// asks for it, on the cluster, and the view shows when the role moved.
	// Promoting the target here directly, as before, lost the writes that
	// reached the old master between the two steps.
	patch := client.MergeFrom(c.DeepCopy())
	if c.Annotations == nil {
		c.Annotations = map[string]string{}
	}
	c.Annotations[switchoverToAnnotation] = body.Pod
	if err := s.ctrlClient.Patch(r.Context(), c, patch); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"requested": body.Pod})
}

// switchoverToAnnotation asks the operator for a switchover to a pod
// (internal/controller.SwitchoverToAnnotation).
const switchoverToAnnotation = "kividb.io/switchover-to"

// postAgent is postAgentJSON, unless a test replaces it.
func (s *server) postAgent(ctx context.Context, podIP, path string, payload any) ([]byte, error) {
	if s.agentPost != nil {
		return s.agentPost(ctx, podIP, path, payload)
	}
	return postAgentJSON(ctx, podIP, path, payload)
}

// clusterEvent records an Event on cluster c, as the operator's own are,
// for an action the GUI took itself. A failure is only logged: the action
// has been taken.
func (s *server) clusterEvent(ctx context.Context, c *kividbv1alpha1.KividbCluster, eventType, reason, message string) {
	now := metav1.Now()
	ev := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s.%x", c.Name, now.UnixNano()), Namespace: c.Namespace},
		InvolvedObject: corev1.ObjectReference{
			APIVersion: kividbv1alpha1.GroupVersion.String(), Kind: "KividbCluster",
			Name: c.Name, Namespace: c.Namespace, UID: c.UID, ResourceVersion: c.ResourceVersion,
		},
		Reason:         reason,
		Message:        message,
		Type:           eventType,
		Source:         corev1.EventSource{Component: "kividb-operator-gui"},
		FirstTimestamp: now,
		LastTimestamp:  now,
		Count:          1,
	}
	if _, err := s.clientset.CoreV1().Events(c.Namespace).Create(ctx, ev, metav1.CreateOptions{}); err != nil {
		log.Printf("recording event %s on %s/%s: %v", reason, c.Namespace, c.Name, err)
	}
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
	if c.Spec.Shards != nil {
		s.snapshotFromCronJob(w, r, c)
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
	agentapi.SetToken(req, agentToken())
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

// snapshotFromCronJob starts a sharded cluster's backup run now: a Job
// from the backup CronJob's template, as `kubectl create job --from`
// does. The operator records it as a KividbSnapshot like a scheduled one.
func (s *server) snapshotFromCronJob(w http.ResponseWriter, r *http.Request, c *kividbv1alpha1.KividbCluster) {
	cj, err := s.clientset.BatchV1().CronJobs(c.Namespace).Get(r.Context(), backupCronJobName(c.Name), metav1.GetOptions{})
	if err != nil {
		writeJSONError(w, http.StatusConflict, fmt.Errorf("the backup CronJob is not there yet: %w", err))
		return
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        cj.Name + "-m" + strconv.FormatInt(time.Now().Unix(), 36),
			Namespace:   c.Namespace,
			Labels:      cj.Spec.JobTemplate.Labels,
			Annotations: map[string]string{"cronjob.kubernetes.io/instantiate": "manual"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "batch/v1", Kind: "CronJob", Name: cj.Name, UID: cj.UID,
				Controller: ptrTo(true), BlockOwnerDeletion: ptrTo(true),
			}},
		},
		Spec: cj.Spec.JobTemplate.Spec,
	}
	created, err := s.clientset.BatchV1().Jobs(c.Namespace).Create(r.Context(), job, metav1.CreateOptions{})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"job": created.Name})
}

func ptrTo[T any](v T) *T { return &v }
