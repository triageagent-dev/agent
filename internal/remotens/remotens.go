// Package remotens is the naming convention that carries a tenant through the
// agent for metrics that arrive from a remote OpenTelemetry Collector.
//
// A remote sender authenticates with a client certificate that names its
// tenants (see the otlpreceiver package). Its namespaces do not exist in this
// cluster, so the annotation lookup that routes local namespaces cannot place
// them. The receiver therefore qualifies each namespace it accepts as
// "<tenant>~<namespace>". The collectors keep working on that string as an
// ordinary namespace, so two tenants that both have a namespace called
// "default" never merge. The push layer reads the tenant back out of it to
// pick the connection, and removes the prefix before the frame leaves the
// agent.
//
// A Kubernetes namespace is a DNS label and cannot contain "~", so no local
// namespace can be mistaken for a qualified one.
package remotens

import (
	"regexp"
	"strings"
)

// Sep separates the tenant from the namespace.
const Sep = "~"

// TenantAttr is the resource attribute a sender sets when its certificate
// names more than one tenant, to say which one a resource belongs to.
const TenantAttr = "triage.tenant"

// tenantName is the shape of a tenant name the agent accepts from a
// certificate: lower case, digits, and "-", "_" or "." inside.
var tenantName = regexp.MustCompile(`^[a-z0-9]([a-z0-9._-]{0,61}[a-z0-9])?$`)

// ValidTenant reports whether name can be used as a tenant in a certificate.
func ValidTenant(name string) bool { return tenantName.MatchString(name) }

// NamespaceKey reports whether a metric attribute key names a namespace
// ("k8s.namespace.name", "k8s.src.namespace"). The bare "namespace" label is
// not one: only cAdvisor series use it as such.
func NamespaceKey(key string) bool {
	return strings.HasSuffix(key, ".namespace") || strings.HasSuffix(key, ".namespace.name")
}

// Qualify returns the internal name of namespace ns of tenant. An empty ns
// gives "<tenant>~", a source with no namespace of its own.
func Qualify(tenant, ns string) string { return tenant + Sep + ns }

// Split returns the tenant and namespace of a qualified name; ok is false for
// an ordinary namespace.
func Split(qualified string) (tenant, ns string, ok bool) {
	tenant, ns, ok = strings.Cut(qualified, Sep)
	if !ok || tenant == "" {
		return "", qualified, false
	}
	return tenant, ns, true
}

// Strip returns the namespace of a name that belongs to tenant, or the name
// unchanged when it is not qualified with that tenant.
func Strip(tenant, qualified string) string {
	if t, ns, ok := Split(qualified); ok && t == tenant {
		return ns
	}
	return qualified
}
