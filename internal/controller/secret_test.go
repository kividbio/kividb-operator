package controller

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
)

func TestRenderACLFile(t *testing.T) {
	t.Parallel()

	t.Run("synthesizes default nopass user when no users", func(t *testing.T) {
		t.Parallel()
		got, err := renderACLFile(nil, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !strings.Contains(got, "user default on nopass ~* &* +@all\n") {
			t.Fatalf("expected open default user, got:\n%s", got)
		}

		empty := &kividbv1alpha1.KividbAclConfig{}
		got2, err := renderACLFile(empty, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !strings.Contains(got2, "user default on nopass ~* &* +@all\n") {
			t.Fatalf("expected open default user for empty acl config, got:\n%s", got2)
		}
	})

	t.Run("uses requirepass for default when set", func(t *testing.T) {
		t.Parallel()
		acl := &kividbv1alpha1.KividbAclConfig{
			Spec: kividbv1alpha1.KividbAclConfigSpec{
				RequirePassSecretRef: &kividbv1alpha1.SecretKeyRef{Name: "auth", Key: "password"},
			},
		}
		secrets := map[string]string{"auth/password": "s3cret"}
		got, err := renderACLFile(acl, secrets)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		sum := sha256.Sum256([]byte("s3cret"))
		wantHash := "#" + hex.EncodeToString(sum[:])
		wantLine := "user default on " + wantHash + " ~* &* +@all\n"
		if !strings.Contains(got, wantLine) {
			t.Fatalf("expected hashed requirepass default user, got:\n%s", got)
		}
		if strings.Contains(got, "nopass") {
			t.Fatalf("nopass should not appear when requirepass set, got:\n%s", got)
		}
	})

	t.Run("renders explicit users with password from secretValues", func(t *testing.T) {
		t.Parallel()
		acl := &kividbv1alpha1.KividbAclConfig{
			Spec: kividbv1alpha1.KividbAclConfigSpec{
				Users: []kividbv1alpha1.KividbUser{
					{
						Name:              "app",
						PasswordSecretRef: &kividbv1alpha1.SecretKeyRef{Name: "creds", Key: "app-pass"},
						KeyPatterns:       []string{"~app:*"},
						ChannelPatterns:   []string{"&app:*"},
						CommandRules:      []string{"+@read", "+@write"},
					},
				},
			},
		}
		secrets := map[string]string{"creds/app-pass": "hunter2"}
		got, err := renderACLFile(acl, secrets)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		sum := sha256.Sum256([]byte("hunter2"))
		wantHash := "#" + hex.EncodeToString(sum[:])
		wantApp := "user app on " + wantHash + " ~app:* &app:* +@read +@write\n"
		if !strings.Contains(got, wantApp) {
			t.Fatalf("expected app user line, got:\n%s", got)
		}
		// Still synthesizes default nopass since no default / requirepass.
		if !strings.Contains(got, "user default on nopass ~* &* +@all\n") {
			t.Fatalf("expected synthesized default user, got:\n%s", got)
		}
	})

	t.Run("errors when password secret key missing", func(t *testing.T) {
		t.Parallel()
		acl := &kividbv1alpha1.KividbAclConfig{
			Spec: kividbv1alpha1.KividbAclConfigSpec{
				Users: []kividbv1alpha1.KividbUser{
					{
						Name:              "app",
						PasswordSecretRef: &kividbv1alpha1.SecretKeyRef{Name: "creds", Key: "missing"},
					},
				},
			},
		}
		_, err := renderACLFile(acl, map[string]string{})
		if err == nil {
			t.Fatal("expected error when password secret key missing")
		}
		if !strings.Contains(err.Error(), "password secret") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("errors when requirepass secret missing", func(t *testing.T) {
		t.Parallel()
		acl := &kividbv1alpha1.KividbAclConfig{
			Spec: kividbv1alpha1.KividbAclConfigSpec{
				RequirePassSecretRef: &kividbv1alpha1.SecretKeyRef{Name: "auth", Key: "password"},
			},
		}
		_, err := renderACLFile(acl, nil)
		if err == nil {
			t.Fatal("expected error when requirepass secret missing")
		}
	})
}
