package agentapi

import (
	"crypto/pbkdf2"
	"crypto/sha256"
	"encoding/hex"
)

// fingerprintIterations keeps a fingerprint cheap to compute for the
// operator's own change detection while not being a fast hash of secret
// material: the ACL file contains password hashes, and the default user's
// password is fingerprinted directly.
const fingerprintIterations = 4096

// Fingerprint returns a deterministic, slow-to-invert digest of data. salt
// separates the uses (and, for passwords, the clusters) from each other.
// It is used only to tell whether something has changed -- never to verify
// a password.
func Fingerprint(data, salt string) string {
	key, err := pbkdf2.Key(sha256.New, data, []byte(salt), fingerprintIterations, 32)
	if err != nil {
		// Only possible with a key length or iteration count out of range,
		// which the constants above rule out.
		panic(err)
	}
	return hex.EncodeToString(key)
}

// AclFileFingerprintSalt is the salt both the controller and the agent use
// for the ACL file, so that the agent can tell whether the file mounted in
// its pod is the one the controller rendered (see AclReloadRequest).
const AclFileFingerprintSalt = "kividb-operator/acl-file"
