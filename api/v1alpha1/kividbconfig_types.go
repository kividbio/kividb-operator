package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// KividbTLSSpec configures TLS for kividb's --tls-port listener. Only
// meaningful when the referencing KividbCluster's spec.variant is "tls" or
// "full" (the base kividb image is not built with TLS support compiled
// in -- see spec.variant on KividbCluster).
type KividbTLSSpec struct {
	// Enabled turns on the tls-port listener alongside the plaintext port.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// Port is the TLS listener port. Defaults to 6443.
	// +optional
	// +kubebuilder:default=6443
	Port int32 `json:"port,omitempty"`

	// CertSecretRef points at a Secret holding the certificate, private
	// key, and (optionally) CA certificate.
	CertSecretRef TLSSecretRef `json:"certSecretRef"`
}

// TLSSecretRef names a Secret and the keys within it holding a TLS
// certificate/key pair, matching the standard "kubernetes.io/tls" Secret
// type's default key names.
type TLSSecretRef struct {
	// Name of the Secret.
	Name string `json:"name"`

	// CertKey is the key holding the PEM certificate. Defaults to "tls.crt".
	// +optional
	CertKey string `json:"certKey,omitempty"`

	// KeyKey is the key holding the PEM private key. Defaults to "tls.key".
	// +optional
	KeyKey string `json:"keyKey,omitempty"`

	// CAKey is the key holding a PEM CA certificate, if client-cert
	// verification is desired. Defaults to "ca.crt". Optional -- if the
	// key doesn't exist in the Secret, no --tls-ca-cert-file is set.
	// +optional
	CAKey string `json:"caKey,omitempty"`
}

// KividbConfigSpec defines a reusable set of kividb.conf directives (and
// TLS settings), referenced by name from one or more KividbClusters via
// spec.configRef -- analogous to StackGres's SGPostgresConfig. This is
// what the operator renders into the ConfigMap mounted at
// /etc/kividb/kividb.conf; you never create that ConfigMap yourself.
type KividbConfigSpec struct {
	// Directives are free-form kividb.conf lines (lower-kebab-case keys,
	// e.g. maxmemory, threads, aof, loglevel, cluster-enabled,
	// notify-keyspace-events, slowlog-log-slower-than). Do not set
	// "replicaof", "port", or "aclfile" here -- those remain
	// operator-managed regardless of which KividbConfig is referenced.
	// +optional
	Directives map[string]string `json:"directives,omitempty"`

	// TLS configures the tls-port listener. Requires a KividbCluster with
	// spec.variant "tls" or "full".
	// +optional
	TLS *KividbTLSSpec `json:"tls,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=kdbc
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// KividbConfig is a reusable, standalone set of kividb.conf directives.
// Create one and reference it by name from any number of KividbClusters'
// spec.configRef.
type KividbConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec KividbConfigSpec `json:"spec"`
}

// +kubebuilder:object:root=true

// KividbConfigList contains a list of KividbConfig.
type KividbConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KividbConfig `json:"items"`
}

func init() {
	SchemeBuilder.Register(&KividbConfig{}, &KividbConfigList{})
}
