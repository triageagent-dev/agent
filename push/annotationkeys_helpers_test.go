package push

import "github.com/triageagent-dev/agent/internal/annotationkeys"

// Shared annotation key names for this package's tests, so fixtures across
// many test files can build map[string]string{annotationTier: "2"} without
// repeating annotationkeys.Tier at every call site.
const (
	annotationTenant             = annotationkeys.Tenant
	annotationTier               = annotationkeys.Tier
	annotationWeight             = annotationkeys.Weight
	annotationDomain             = annotationkeys.Domain
	annotationDescription        = annotationkeys.Description
	annotationVirtualApp         = annotationkeys.VirtualApp
	annotationVirtualAppGroup    = annotationkeys.VirtualAppGroup
	annotationVirtualAppIdentity = annotationkeys.VirtualAppIdentity
)
