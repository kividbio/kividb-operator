package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/kividbio/kividb-operator/internal/agentapi"
)

// AgentClient talks to the per-pod agent sidecar's HTTP API. One instance
// is shared across every Reconcile call; it is safe for concurrent use
// (http.Client is).
type AgentClient struct {
	http *http.Client
}

// NewAgentClient builds an AgentClient with the given per-call timeout.
func NewAgentClient(timeout time.Duration) *AgentClient {
	return &AgentClient{http: &http.Client{Timeout: timeout}}
}

func (a *AgentClient) url(podIP, path string) string {
	return fmt.Sprintf("http://%s%s", agentAddr(podIP), path)
}

func (a *AgentClient) Status(ctx context.Context, podIP string) (*agentapi.StatusResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.url(podIP, "/status"), nil)
	if err != nil {
		return nil, err
	}
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
	var body io.Reader
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.url(podIP, path), body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		var errResp agentapi.ErrorResponse
		if jsonErr := json.Unmarshal(respBody, &errResp); jsonErr == nil && errResp.Error != "" {
			return fmt.Errorf("agent %s %s: %s", podIP, path, errResp.Error)
		}
		return fmt.Errorf("agent %s %s: status %d: %s", podIP, path, resp.StatusCode, string(respBody))
	}
	return nil
}

func (a *AgentClient) Promote(ctx context.Context, podIP string) error {
	return a.postAction(ctx, podIP, "/promote", nil)
}

func (a *AgentClient) ReplicaOf(ctx context.Context, podIP, masterHost string, masterPort int32) error {
	return a.postAction(ctx, podIP, "/replicaof", agentapi.ReplicaOfRequest{Host: masterHost, Port: masterPort})
}

func (a *AgentClient) AclReload(ctx context.Context, podIP string) error {
	return a.postAction(ctx, podIP, "/acl/reload", nil)
}
