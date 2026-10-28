package main

import (
	"os"
	"strings"
	"sync"
	"time"

	"github.com/kividbio/kividb-operator/internal/agentapi"
)

// defaultAgentTokenFile is where the chart mounts the operator's agent
// token Secret. The GUI reads it as a file, never through the Kubernetes
// API: its RBAC has no access to Secrets.
const defaultAgentTokenFile = "/etc/kividb-agent-token/" + agentapi.TokenSecretKey

// agentTokenRefresh is how often the file is read again: the operator
// creates the Secret at its first start, possibly after the GUI's, and the
// kubelet then fills in the (optional) mount.
const agentTokenRefresh = 30 * time.Second

var agentTokenCache struct {
	sync.Mutex
	value string
	read  time.Time
}

// agentToken is the token the GUI sends to every agent: AGENT_TOKEN if set,
// else the contents of AGENT_TOKEN_FILE (default defaultAgentTokenFile).
func agentToken() string {
	if t := os.Getenv(agentapi.TokenEnv); t != "" {
		return t
	}
	agentTokenCache.Lock()
	defer agentTokenCache.Unlock()
	if agentTokenCache.value != "" && time.Since(agentTokenCache.read) < agentTokenRefresh {
		return agentTokenCache.value
	}
	path := os.Getenv("AGENT_TOKEN_FILE")
	if path == "" {
		path = defaultAgentTokenFile
	}
	if b, err := os.ReadFile(path); err == nil {
		agentTokenCache.value = strings.TrimSpace(string(b))
		agentTokenCache.read = time.Now()
	}
	return agentTokenCache.value
}
