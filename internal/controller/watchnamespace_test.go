package controller

import (
	"reflect"
	"strings"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/cache"
)

func TestParseWatchNamespaces(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   string
		want    []string
		wantErr string
	}{
		{name: "unset watches every namespace", value: "", want: nil},
		{name: "one namespace", value: "team-a", want: []string{"team-a"}},
		{name: "OLM MultiNamespace list", value: "team-a,team-b", want: []string{"team-a", "team-b"}},
		{name: "spaces around names", value: " team-a , team-b ", want: []string{"team-a", "team-b"}},
		{name: "repeated name kept once", value: "team-a,team-b,team-a", want: []string{"team-a", "team-b"}},
		{name: "empty items skipped", value: "team-a,,team-b,", want: []string{"team-a", "team-b"}},
		{name: "blank is not all namespaces", value: "  ", wantErr: "names no namespace"},
		{name: "upper case", value: "Team-A", wantErr: `"Team-A"`},
		{name: "wildcard", value: "*", wantErr: `"*"`},
		{name: "one bad name fails the whole list", value: "team-a,team_b", wantErr: `"team_b"`},
		{name: "too long", value: strings.Repeat("a", 64), wantErr: "must be no more than 63"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseWatchNamespaces(tt.value)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("ParseWatchNamespaces(%q) error = %v, want one containing %q", tt.value, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseWatchNamespaces(%q) error = %v", tt.value, err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("ParseWatchNamespaces(%q) = %q, want %q", tt.value, got, tt.want)
			}
		})
	}
}

func TestWatchNamespacesCacheOptions(t *testing.T) {
	t.Parallel()

	if got := WatchNamespacesCacheOptions(nil).DefaultNamespaces; got != nil {
		t.Fatalf("no namespaces: DefaultNamespaces = %v, want nil (every namespace)", got)
	}

	got := WatchNamespacesCacheOptions([]string{"team-a", "team-b"}).DefaultNamespaces
	want := map[string]cache.Config{"team-a": {}, "team-b": {}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("DefaultNamespaces = %v, want %v", got, want)
	}
}
