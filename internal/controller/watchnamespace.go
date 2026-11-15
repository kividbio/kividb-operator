package controller

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/cache"
)

// ParseWatchNamespaces reads the WATCH_NAMESPACE value: a comma-separated
// list of namespaces, the format OLM writes into the olm.targetNamespaces
// annotation. An empty value means every namespace and returns nil.
//
// A value that is set but names no namespace (" ", ",") is an error, not
// "every namespace": whoever set it meant to restrict the operator.
func ParseWatchNamespaces(value string) ([]string, error) {
	if value == "" {
		return nil, nil
	}
	var namespaces []string
	seen := map[string]bool{}
	for _, item := range strings.Split(value, ",") {
		ns := strings.TrimSpace(item)
		if ns == "" || seen[ns] {
			continue
		}
		if errs := validation.IsDNS1123Label(ns); len(errs) > 0 {
			return nil, fmt.Errorf("WATCH_NAMESPACE: %q is not a namespace name: %s", ns, strings.Join(errs, "; "))
		}
		seen[ns] = true
		namespaces = append(namespaces, ns)
	}
	if len(namespaces) == 0 {
		return nil, fmt.Errorf("WATCH_NAMESPACE=%q names no namespace; unset it to watch every namespace", value)
	}
	return namespaces, nil
}

// WatchNamespacesCacheOptions limits the manager's cache to namespaces.
// With none it leaves the cache cluster-wide.
func WatchNamespacesCacheOptions(namespaces []string) cache.Options {
	if len(namespaces) == 0 {
		return cache.Options{}
	}
	defaults := make(map[string]cache.Config, len(namespaces))
	for _, ns := range namespaces {
		defaults[ns] = cache.Config{}
	}
	return cache.Options{DefaultNamespaces: defaults}
}
