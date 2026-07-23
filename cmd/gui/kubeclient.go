package main

import (
	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// scheme only needs to know about KividbCluster: it backs the
// controller-runtime client used exclusively for that CRD (see
// newControllerRuntimeClient). Pods/Services/StatefulSets/CronJobs/Events
// are fetched through the plain client-go clientset instead, since typed
// generated clients already exist for them in k8s.io/client-go.
var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(kividbv1alpha1.AddToScheme(scheme))
}

// loadKubeConfig builds a *rest.Config, preferring in-cluster config (the
// normal case when running as a Deployment) and falling back to the local
// kubeconfig loading rules (KUBECONFIG env var, then ~/.kube/config) so
// `go run ./cmd/gui` works against a developer's current context outside a
// cluster.
func loadKubeConfig() (*rest.Config, error) {
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}

	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	overrides := &clientcmd.ConfigOverrides{}
	kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, overrides)
	return kubeConfig.ClientConfig()
}

// newControllerRuntimeClient returns a direct (non-caching) controller-runtime
// client scoped to the KividbCluster CRD. A direct client is intentional
// here: the GUI is a low-traffic, read-only dashboard, so there is no need
// to pay for an informer cache's startup list/watch cost.
func newControllerRuntimeClient(cfg *rest.Config) (client.Client, error) {
	return client.New(cfg, client.Options{Scheme: scheme})
}

// newClientset returns a plain client-go typed clientset used for Pods,
// Services, StatefulSets, CronJobs and Events. The GUI never requests
// Secrets through this (or any other) client -- see docs/GUI.md.
func newClientset(cfg *rest.Config) (kubernetes.Interface, error) {
	return kubernetes.NewForConfig(cfg)
}
