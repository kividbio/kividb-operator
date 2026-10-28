package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/kividbio/kividb-operator/internal/agentapi"
)

// AgentClient talks to the per-pod agent sidecar's HTTP API. One instance
// is shared across every Reconcile call; it is safe for concurrent use
// (http.Client is).
type AgentClient struct {
	http *http.Client
	// Token is the agent token: sent with every call, and copied into each
	// cluster's auth Secret for its agents to require.
	Token string
}

// NewAgentClient builds an AgentClient with the given per-call timeout and
// agent token.
func NewAgentClient(timeout time.Duration, token string) *AgentClient {
	return &AgentClient{http: &http.Client{Timeout: timeout}, Token: token}
}

func (a *AgentClient) url(podIP, path string) string {
	return fmt.Sprintf("http://%s%s", agentAddr(podIP), path)
}

func (a *AgentClient) Status(ctx context.Context, podIP string) (*agentapi.StatusResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.url(podIP, "/status"), nil)
	if err != nil {
		return nil, err
	}
	agentapi.SetToken(req, a.Token)
	resp, err := a.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("agent %s: status %d: %s", podIP, resp.StatusCode, string(body))
	}
	var out agentapi.StatusResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("agent %s: decode status: %w", podIP, err)
	}
	return &out, nil
}

func (a *AgentClient) postAction(ctx context.Context, podIP, path string, payload any) error {
	_, err := a.post(ctx, podIP, path, payload)
	return err
}

// post returns the HTTP status alongside the error so a caller can tell a
// specific non-200 answer apart from a transport failure; status is 0 if
// no response was received.
func (a *AgentClient) post(ctx context.Context, podIP, path string, payload any) (status int, err error) {
	var body io.Reader
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return 0, err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.url(podIP, path), body)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	agentapi.SetToken(req, a.Token)
	resp, err := a.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		var errResp agentapi.ErrorResponse
		if jsonErr := json.Unmarshal(respBody, &errResp); jsonErr == nil && errResp.Error != "" {
			return resp.StatusCode, fmt.Errorf("agent %s %s: %s", podIP, path, errResp.Error)
		}
		return resp.StatusCode, fmt.Errorf("agent %s %s: status %d: %s", podIP, path, resp.StatusCode, string(respBody))
	}
	return resp.StatusCode, nil
}

// postJSON posts payload and decodes a 200 answer into out.
func (a *AgentClient) postJSON(ctx context.Context, podIP, path string, payload, out any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.url(podIP, path), bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	agentapi.SetToken(req, a.Token)
	resp, err := a.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("agent %s %s: %w", podIP, path, agentError(resp.StatusCode, body))
	}
	return json.Unmarshal(body, out)
}

// agentError is the error an agent answered with.
func agentError(status int, body []byte) error {
	var e agentapi.ErrorResponse
	if json.Unmarshal(body, &e) == nil && e.Error != "" {
		return fmt.Errorf("%s", e.Error)
	}
	return fmt.Errorf("status %d: %s", status, string(body))
}

// ClusterStatus is the pod's view of the cluster (GET /cluster/status).
func (a *AgentClient) ClusterStatus(ctx context.Context, podIP string) (*agentapi.ClusterStatusResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.url(podIP, "/cluster/status"), nil)
	if err != nil {
		return nil, err
	}
	agentapi.SetToken(req, a.Token)
	resp, err := a.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("agent %s: cluster status %d: %s", podIP, resp.StatusCode, string(body))
	}
	var out agentapi.ClusterStatusResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("agent %s: decode cluster status: %w", podIP, err)
	}
	return &out, nil
}

func (a *AgentClient) ClusterMeet(ctx context.Context, podIP, ip string, port int) error {
	return a.postAction(ctx, podIP, "/cluster/meet", agentapi.ClusterMeetRequest{IP: ip, Port: port})
}

func (a *AgentClient) ClusterAddSlots(ctx context.Context, podIP string, ranges [][2]int) error {
	return a.postAction(ctx, podIP, "/cluster/addslots", agentapi.ClusterSlotsRequest{Ranges: ranges})
}

func (a *AgentClient) ClusterReplicate(ctx context.Context, podIP, masterID string) error {
	return a.postAction(ctx, podIP, "/cluster/replicate", agentapi.ClusterNodeRequest{NodeID: masterID})
}

func (a *AgentClient) ClusterForget(ctx context.Context, podIP, nodeID string) error {
	return a.postAction(ctx, podIP, "/cluster/forget", agentapi.ClusterNodeRequest{NodeID: nodeID})
}

// ClusterFailover asks the replica at podIP to take over from its master
// (CLUSTER FAILOVER, the coordinated kind: the master pauses its clients
// until the replica has caught up).
func (a *AgentClient) ClusterFailover(ctx context.Context, podIP string) error {
	return a.postAction(ctx, podIP, "/cluster/failover", nil)
}

// ClusterMigrationImport starts moving ranges, all owned by one other
// master, to the master at podIP, and returns the engine's task ID.
func (a *AgentClient) ClusterMigrationImport(ctx context.Context, podIP string, ranges [][2]int) (string, error) {
	var out agentapi.ClusterImportResponse
	err := a.postJSON(ctx, podIP, "/cluster/migration/import", agentapi.ClusterSlotsRequest{Ranges: ranges}, &out)
	return out.TaskID, err
}

// ClusterMigrationStatus reports a migration task on the master at
// podIP; found is false when that node has no such task (it restarted).
func (a *AgentClient) ClusterMigrationStatus(ctx context.Context, podIP, taskID string) (task agentapi.ClusterMigrationTask, found bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.url(podIP, "/cluster/migration/status?id="+url.QueryEscape(taskID)), nil)
	if err != nil {
		return task, false, err
	}
	agentapi.SetToken(req, a.Token)
	resp, err := a.http.Do(req)
	if err != nil {
		return task, false, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	switch resp.StatusCode {
	case http.StatusOK:
		return task, true, json.Unmarshal(body, &task)
	case http.StatusNotFound:
		return task, false, nil
	}
	return task, false, agentError(resp.StatusCode, body)
}

func (a *AgentClient) ClusterAnnounce(ctx context.Context, podIP, hostname string) error {
	return a.postAction(ctx, podIP, "/cluster/announce", agentapi.ClusterAnnounceRequest{Hostname: hostname})
}

func (a *AgentClient) Promote(ctx context.Context, podIP string) error {
	return a.postAction(ctx, podIP, "/promote", nil)
}

func (a *AgentClient) ReplicaOf(ctx context.Context, podIP, masterHost string, masterPort int32) error {
	return a.postAction(ctx, podIP, "/replicaof", agentapi.ReplicaOfRequest{Host: masterHost, Port: masterPort})
}

// AclReload asks the pod's kividb to re-read its ACL file, but only if the
// file mounted in that pod already hashes to fileHash. It returns
// reloaded=false with no error when it does not yet (the agent's 409), so
// the caller can simply try again later.
func (a *AgentClient) AclReload(ctx context.Context, podIP, fileHash string) (reloaded bool, err error) {
	status, err := a.post(ctx, podIP, "/acl/reload", agentapi.AclReloadRequest{IfFileHash: fileHash})
	if status == http.StatusConflict {
		return false, nil
	}
	return err == nil, err
}
