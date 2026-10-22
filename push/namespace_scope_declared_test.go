package push

import "testing"

// The core renders an empty hello namespace list as "all", so a cluster-mode
// agent must declare the scope it actually observes or it claims cluster-wide
// coverage it does not have.
func TestDeclaredNamespaces(t *testing.T) {
	for _, tc := range []struct {
		name       string
		configured []string
		cache      []string
		want       []string
	}{
		{"explicit list wins", []string{"a", "b"}, []string{"c"}, []string{"a", "b"}},
		{"cluster mode falls back to the cache scope", nil, []string{"tenant-a", "tenant-b"}, []string{"tenant-a", "tenant-b"}},
		{"nothing known stays empty", nil, nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &Pusher{opts: Options{Namespaces: tc.configured, CacheNamespaces: tc.cache}}
			got := p.declaredNamespaces()
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("got %v, want %v", got, tc.want)
				}
			}
		})
	}
}
