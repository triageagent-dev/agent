package remotens

import "testing"

func TestQualifySplitStrip(t *testing.T) {
	q := Qualify("acme", "default")
	if q != "acme~default" {
		t.Fatalf("Qualify = %q", q)
	}
	if tn, ns, ok := Split(q); !ok || tn != "acme" || ns != "default" {
		t.Fatalf("Split = %q %q %v", tn, ns, ok)
	}
	if tn, ns, ok := Split(Qualify("acme", "")); !ok || tn != "acme" || ns != "" {
		t.Fatalf("Split of a namespace-less source = %q %q %v", tn, ns, ok)
	}
	for _, local := range []string{"default", "kube-system", "", "~x"} {
		if _, _, ok := Split(local); ok {
			t.Fatalf("%q split as qualified", local)
		}
	}
	if Strip("acme", q) != "default" || Strip("globex", q) != q || Strip("acme", "default") != "default" {
		t.Fatal("Strip removed the wrong prefix")
	}
}

func TestValidTenant(t *testing.T) {
	for _, ok := range []string{"a", "acme", "acme-eu", "a_b", "a.b", "t1"} {
		if !ValidTenant(ok) {
			t.Fatalf("%q rejected", ok)
		}
	}
	for _, bad := range []string{"", "Acme", "a~b", "-a", "a-", "a/b", "a b"} {
		if ValidTenant(bad) {
			t.Fatalf("%q accepted", bad)
		}
	}
}
