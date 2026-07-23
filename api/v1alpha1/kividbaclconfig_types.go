package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// KividbAclConfigSpec defines a reusable set of ACL users (and/or a
// requirepass), referenced by name from one or more KividbClusters via
// spec.aclConfigRef. This is what the operator renders into the ACL file
// Secret mounted at /etc/kividb/acl/users.acl; you never create that
// Secret yourself.
type KividbAclConfigSpec struct {
	// RequirePassSecretRef points at a Secret key holding the requirepass
	// value for the built-in default user. If unset and no "default" user
	// is defined explicitly in Users, unauthenticated access is allowed.
	// +optional
	RequirePassSecretRef *SecretKeyRef `json:"requirePassSecretRef,omitempty"`

	// Users is the list of ACL users to render into the ACL file.
	// +optional
	Users []KividbUser `json:"users,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=kdbacl
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// KividbAclConfig is a reusable, standalone set of ACL users. Create one
// and reference it by name from any number of KividbClusters'
// spec.aclConfigRef.
type KividbAclConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec KividbAclConfigSpec `json:"spec"`
}

// +kubebuilder:object:root=true

// KividbAclConfigList contains a list of KividbAclConfig.
type KividbAclConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KividbAclConfig `json:"items"`
}

func init() {
	SchemeBuilder.Register(&KividbAclConfig{}, &KividbAclConfigList{})
}
