package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/kividbio/kividb-operator/internal/agentapi"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// DefaultAgentTokenSecret is the Secret, in the operator's namespace, that
// holds the agent token when --agent-token-secret does not name another.
const DefaultAgentTokenSecret = "kividb-operator-agent-token"

// EnsureAgentToken returns the agent token kept in the Secret
// namespace/name, creating the Secret with a random token if it does not
// exist. Every cluster's agents require that token (it is copied into each
// cluster's auth Secret); the operator and the GUI send it.
//
// It is called once at startup, before the manager's cache runs, so c must
// be a direct (uncached) client.
func EnsureAgentToken(ctx context.Context, c client.Client, namespace, name string) (string, error) {
	var s corev1.Secret
	err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &s)
	if apierrors.IsNotFound(err) {
		b := make([]byte, 32)
		if _, err := rand.Read(b); err != nil {
			return "", fmt.Errorf("generating the agent token: %w", err)
		}
		s = corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
				Labels:    map[string]string{"app.kubernetes.io/managed-by": managedByValue},
			},
			Type: corev1.SecretTypeOpaque,
			Data: map[string][]byte{agentapi.TokenSecretKey: []byte(hex.EncodeToString(b))},
		}
		err = c.Create(ctx, &s)
		if apierrors.IsAlreadyExists(err) {
			// Another replica of the operator made it first.
			err = c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &s)
		}
	}
	if err != nil {
		return "", fmt.Errorf("agent token Secret %s/%s: %w", namespace, name, err)
	}
	token := string(s.Data[agentapi.TokenSecretKey])
	if token == "" {
		return "", fmt.Errorf("agent token Secret %s/%s has no %q", namespace, name, agentapi.TokenSecretKey)
	}
	return token, nil
}
