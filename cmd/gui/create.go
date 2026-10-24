package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

// maxClusterNameLen leaves room for the names the operator derives from a
// cluster's: "<name>-replicas", "<name>-backup-<timestamp>" and the
// StatefulSet's pod names all have to stay valid.
const maxClusterNameLen = 40

// maxCreateReplicas is the most replicas the form offers. The CRD allows
// more; past this, kubectl is the better tool.
const maxCreateReplicas = 9

// createRequest is the new-cluster form. Everything but Namespace and Name
// has a default.
type createRequest struct {
	Namespace         string `json:"namespace"`
	Name              string `json:"name"`
	Replicas          *int32 `json:"replicas,omitempty"`
	Variant           string `json:"variant,omitempty"`
	Image             string `json:"image,omitempty"` // overrides the image the variant picks
	StorageSize       string `json:"storageSize,omitempty"`
	StorageClassName  string `json:"storageClassName,omitempty"`
	Memory            string `json:"memory,omitempty"` // request and limit
	CPU               string `json:"cpu,omitempty"`    // request
	SnapshotConfig    string `json:"snapshotConfig,omitempty"`
	MasterServiceType string `json:"masterServiceType,omitempty"`
	Monitoring        bool   `json:"monitoring,omitempty"`
}

// variantImage is the default engine image for a variant: the operator
// never derives an image from spec.variant, so the form sets both.
func variantImage(variant kividbv1alpha1.KividbVariant) string {
	if variant == kividbv1alpha1.VariantStandard {
		return defaultKividbImage
	}
	return defaultKividbImage + "-" + string(variant)
}

func parseVariant(s string) (kividbv1alpha1.KividbVariant, error) {
	switch v := kividbv1alpha1.KividbVariant(s); v {
	case "":
		return kividbv1alpha1.VariantStandard, nil
	case kividbv1alpha1.VariantStandard, kividbv1alpha1.VariantTLS, kividbv1alpha1.VariantLua, kividbv1alpha1.VariantFull:
		return v, nil
	}
	return "", fmt.Errorf("variant must be standard, tls, lua or full, not %q", s)
}

// buildCluster turns the form into a KividbCluster, or says what is wrong
// with it.
func buildCluster(req createRequest) (*kividbv1alpha1.KividbCluster, error) {
	req.Namespace = strings.TrimSpace(req.Namespace)
	req.Name = strings.TrimSpace(req.Name)
	if errs := validation.IsDNS1123Label(req.Namespace); len(errs) > 0 {
		return nil, fmt.Errorf("namespace %q: %s", req.Namespace, strings.Join(errs, "; "))
	}
	if errs := validation.IsDNS1123Label(req.Name); len(errs) > 0 {
		return nil, fmt.Errorf("name %q: %s", req.Name, strings.Join(errs, "; "))
	}
	if len(req.Name) > maxClusterNameLen {
		return nil, fmt.Errorf("name %q: at most %d characters, so the names derived from it stay valid", req.Name, maxClusterNameLen)
	}

	replicas := int32(2)
	if req.Replicas != nil {
		replicas = *req.Replicas
	}
	if replicas < 0 || replicas > maxCreateReplicas {
		return nil, fmt.Errorf("replicas must be between 0 and %d", maxCreateReplicas)
	}

	variant, err := parseVariant(req.Variant)
	if err != nil {
		return nil, err
	}
	image := strings.TrimSpace(req.Image)
	if image == "" {
		image = variantImage(variant)
	}

	storage := req.StorageSize
	if storage == "" {
		storage = "1Gi"
	}
	if _, err := resource.ParseQuantity(storage); err != nil {
		return nil, fmt.Errorf("storage size %q: %v", storage, err)
	}

	c := &kividbv1alpha1.KividbCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: kividbv1alpha1.GroupVersion.String(),
			Kind:       "KividbCluster",
		},
		ObjectMeta: metav1.ObjectMeta{Namespace: req.Namespace, Name: req.Name},
		Spec: kividbv1alpha1.KividbClusterSpec{
			Replicas: replicas,
			Image:    image,
			Variant:  variant,
			Storage:  kividbv1alpha1.StorageSpec{Size: storage},
		},
	}
	if sc := strings.TrimSpace(req.StorageClassName); sc != "" {
		c.Spec.Storage.StorageClassName = &sc
	}

	if req.Memory != "" || req.CPU != "" {
		c.Spec.Resources = corev1.ResourceRequirements{Requests: corev1.ResourceList{}}
		if req.Memory != "" {
			q, err := resource.ParseQuantity(req.Memory)
			if err != nil {
				return nil, fmt.Errorf("memory %q: %v", req.Memory, err)
			}
			c.Spec.Resources.Requests[corev1.ResourceMemory] = q
			c.Spec.Resources.Limits = corev1.ResourceList{corev1.ResourceMemory: q}
		}
		if req.CPU != "" {
			q, err := resource.ParseQuantity(req.CPU)
			if err != nil {
				return nil, fmt.Errorf("cpu %q: %v", req.CPU, err)
			}
			c.Spec.Resources.Requests[corev1.ResourceCPU] = q
		}
	}

	if sc := strings.TrimSpace(req.SnapshotConfig); sc != "" {
		c.Spec.SnapshotConfigRef = &corev1.LocalObjectReference{Name: sc}
	}

	switch corev1.ServiceType(req.MasterServiceType) {
	case "", corev1.ServiceTypeClusterIP:
	case corev1.ServiceTypeNodePort, corev1.ServiceTypeLoadBalancer:
		c.Spec.Services.Master.Type = corev1.ServiceType(req.MasterServiceType)
	default:
		return nil, fmt.Errorf("master service type must be ClusterIP, NodePort or LoadBalancer, not %q", req.MasterServiceType)
	}

	c.Spec.Monitoring.Enabled = req.Monitoring
	return c, nil
}

// clusterYAML is the manifest a user would apply for c: what they asked for
// (and, after a dry run, the API server's defaults), without the server's
// bookkeeping or the empty sections Go's zero values would add.
func clusterYAML(c *kividbv1alpha1.KividbCluster) ([]byte, error) {
	b, err := json.Marshal(c.Spec)
	if err != nil {
		return nil, err
	}
	var spec map[string]any
	if err := json.Unmarshal(b, &spec); err != nil {
		return nil, err
	}
	pruneEmpty(spec)
	return yaml.Marshal(map[string]any{
		"apiVersion": kividbv1alpha1.GroupVersion.String(),
		"kind":       "KividbCluster",
		"metadata":   map[string]any{"name": c.Name, "namespace": c.Namespace},
		"spec":       spec,
	})
}

// pruneEmpty removes nulls, and maps left empty, from m, depth first.
func pruneEmpty(m map[string]any) {
	for k, v := range m {
		switch x := v.(type) {
		case nil:
			delete(m, k)
		case map[string]any:
			pruneEmpty(x)
			if len(x) == 0 {
				delete(m, k)
			}
		}
	}
}

// handleAPICreateCluster creates a KividbCluster from the form. With
// ?dryRun=1 the API server validates and defaults it without storing it,
// and the reply is the manifest as YAML: the form's preview.
func (s *server) handleAPICreateCluster(w http.ResponseWriter, r *http.Request) {
	var req createRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, fmt.Errorf("decode request: %w", err))
		return
	}
	if req.Namespace == "" && s.watchNamespace != "" {
		req.Namespace = s.watchNamespace
	}
	c, err := buildCluster(req)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err)
		return
	}
	if !s.requireNS(w, c.Namespace) {
		return
	}

	opts := []client.CreateOption{}
	dryRun := r.URL.Query().Get("dryRun") != ""
	if dryRun {
		opts = append(opts, client.DryRunAll)
	}
	if err := s.ctrlClient.Create(r.Context(), c, opts...); err != nil {
		status := http.StatusInternalServerError
		switch {
		case apierrors.IsAlreadyExists(err):
			status = http.StatusConflict
		case apierrors.IsInvalid(err), apierrors.IsBadRequest(err):
			status = http.StatusBadRequest
		case apierrors.IsForbidden(err):
			status = http.StatusForbidden
		case apierrors.IsNotFound(err):
			// The namespace does not exist.
			status = http.StatusBadRequest
		}
		writeJSONError(w, status, err)
		return
	}
	out, err := clusterYAML(c)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err)
		return
	}
	status := http.StatusCreated
	if dryRun {
		status = http.StatusOK
	}
	writeJSON(w, status, map[string]any{
		"namespace": c.Namespace,
		"name":      c.Name,
		"dryRun":    dryRun,
		"yaml":      string(out),
	})
}

type storageClassOption struct {
	Name    string `json:"name"`
	Default bool   `json:"default,omitempty"`
}

// handleAPICreateOptions lists what the form offers to choose from in a
// namespace: its snapshot configs, the cluster's storage classes, the
// variants and their images. Anything it cannot read is just left out.
func (s *server) handleAPICreateOptions(w http.ResponseWriter, r *http.Request) {
	namespace := r.URL.Query().Get("namespace")
	if namespace == "" {
		namespace = s.watchNamespace
	}
	if namespace == "" {
		namespace = "default"
	}
	if !s.requireNS(w, namespace) {
		return
	}

	snapshotConfigs := []string{}
	var cfgs kividbv1alpha1.KividbSnapshotConfigList
	if err := s.ctrlClient.List(r.Context(), &cfgs, client.InNamespace(namespace)); err == nil {
		for _, c := range cfgs.Items {
			snapshotConfigs = append(snapshotConfigs, c.Name)
		}
		sort.Strings(snapshotConfigs)
	}

	storageClasses := []storageClassOption{}
	if list, err := s.clientset.StorageV1().StorageClasses().List(r.Context(), metav1.ListOptions{}); err == nil {
		for _, sc := range list.Items {
			storageClasses = append(storageClasses, storageClassOption{
				Name:    sc.Name,
				Default: sc.Annotations["storageclass.kubernetes.io/is-default-class"] == "true",
			})
		}
		sort.Slice(storageClasses, func(i, j int) bool { return storageClasses[i].Name < storageClasses[j].Name })
	}

	variants := []map[string]string{}
	for _, v := range []kividbv1alpha1.KividbVariant{
		kividbv1alpha1.VariantStandard, kividbv1alpha1.VariantLua, kividbv1alpha1.VariantTLS, kividbv1alpha1.VariantFull,
	} {
		variants = append(variants, map[string]string{"name": string(v), "image": variantImage(v)})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"namespace":       namespace,
		"snapshotConfigs": snapshotConfigs,
		"storageClasses":  storageClasses,
		"variants":        variants,
		"maxReplicas":     maxCreateReplicas,
	})
}
