package agentapi

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// TokenEnv is the environment variable the agent, the backup-trigger Job
// and the GUI read the agent token from.
const TokenEnv = "AGENT_TOKEN"

// TokenSecretKey is the key of the agent token in a KividbCluster's auth
// Secret (and in the operator's own token Secret).
const TokenSecretKey = "agent-token"

// SetToken puts token on req as a bearer token. An empty token sets nothing.
func SetToken(req *http.Request, token string) {
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
}

// TokenMatches reports whether req carries want as its bearer token. An
// empty want never matches.
func TokenMatches(req *http.Request, want string) bool {
	if want == "" {
		return false
	}
	got, ok := strings.CutPrefix(req.Header.Get("Authorization"), "Bearer ")
	return ok && subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}
