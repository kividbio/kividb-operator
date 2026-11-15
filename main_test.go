package main

import (
	"reflect"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/cache"
)

func TestManagerOptionsCache(t *testing.T) {
	t.Parallel()

	if got := managerOptions(":8080", ":8081", false, nil).Cache.DefaultNamespaces; got != nil {
		t.Fatalf("no WATCH_NAMESPACE: DefaultNamespaces = %v, want nil (every namespace)", got)
	}

	got := managerOptions(":8080", ":8081", false, []string{"team-a"}).Cache.DefaultNamespaces
	want := map[string]cache.Config{"team-a": {}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("DefaultNamespaces = %v, want %v", got, want)
	}
}
