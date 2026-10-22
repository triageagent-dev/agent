// Namespace-level triageagent.dev/* annotation inheritance (docs/todo/virtual-apps.md
// namespace-inheritance addendum, 2026-08-24).
//
// Operators can annotate the NAMESPACE object itself, once, with the same
// triageagent.dev/domain, triageagent.dev/description, triageagent.dev/tier, triageagent.dev/weight keys
// workloads already carry (virtualapps.go), plus triageagent.dev/virtual-app: "false"
// to opt the WHOLE namespace out of virtual-app synthesis. Every workload in
// that namespace (grouping/synthesis) inherits any of the four metadata
// fields it does not declare itself. Precedence is strict: workload
// annotation > namespace annotation > absent — a namespace default only ever
// fills a gap, never overrides a value already declared closer to the
// workload.
//
// A second annotation, triageagent.dev/virtual-app-identity (virtual-app-identity
// addendum), selects which chain GroupNameFor uses to name every workload's
// vapp WITHIN this namespace: the default is the workload's own name;
// "labels" opts into the part-of/instance/name label chain instead
// (push/virtualapps.go's IdentityModeWorkload/IdentityModeLabels). It has no
// per-workload counterpart — triageagent.dev/virtual-app-group still outranks it
// either way — and is orthogonal to every annotation above.
package push

import (
	"sync"

	"github.com/triageagent-dev/agent/internal/annotationkeys"
)

// annotationkeys.VirtualAppIdentity, on the NAMESPACE object, selects the
// identity chain GroupNameFor uses for every workload in that namespace
// (push/virtualapps.go's IdentityModeLabels/IdentityModeWorkload). Absence,
// or exactly "workload", selects IdentityModeWorkload, the default; exactly
// "labels" opts into IdentityModeLabels; any other value is treated as the
// default (IdentityModeWorkload) with the value reported invalid
// (IdentityModeInvalid, below) so the caller can warn once. Never meaningful
// on a workload.

// namespaceDefaults is the per-namespace triageagent.dev/* fallback plus the
// namespace-level virtual-app-synthesis opt-out, parsed once per Namespace
// informer event and stored flat by namespace name.
type namespaceDefaults struct {
	// Excluded mirrors triageagent.dev/virtual-app: "false" declared on the NAMESPACE
	// object. Unlike the per-workload annotation of the same name (which
	// only opts ITSELF out), this suppresses vapp synthesis for EVERY
	// workload in the namespace regardless of that workload's own
	// annotation — a workload-level triageagent.dev/virtual-app: "false" keeps
	// working independently in namespaces that are NOT excluded.
	Excluded bool
	// TriageExcluded mirrors annotationkeys.Triage: "false" on the NAMESPACE
	// (exclusion.go): every workload in it is out of triage entirely, and
	// a workload-level "true" does not override it — the same OR rule as
	// Excluded above. It also sets Excluded.
	TriageExcluded bool
	// IdentityMode mirrors triageagent.dev/virtual-app-identity declared on the
	// NAMESPACE object — either "" (absent, or an invalid value: resolves
	// identically to IdentityModeWorkload — see GroupNameFor) or
	// push/virtualapps.go's IdentityModeLabels, never the raw annotation
	// string.
	IdentityMode string
	// IdentityModeInvalid mirrors TierInvalid/WeightInvalid below: present
	// but neither "workload", "labels", nor absent (case-sensitive), so the
	// caller logs it once and IdentityMode is left at "" (the default,
	// IdentityModeWorkload).
	IdentityModeInvalid bool
	Domain              string
	Description         string
	Tier                int
	HasTier             bool
	// TierInvalid mirrors workloadInfo.TierInvalid — present but unparsable,
	// so the caller can log it once and otherwise treat the tier as absent.
	TierInvalid bool
	// Weight mirrors workloadInfo.Weight — the namespace-declared
	// triageagent.dev/weight fallback (docs/todo/service-weight.md). Only
	// meaningful when HasWeight is true.
	Weight float64
	// HasWeight mirrors workloadInfo.HasWeight.
	HasWeight bool
	// WeightInvalid mirrors workloadInfo.WeightInvalid — present but
	// unparsable, so the caller can log it once and otherwise treat the
	// weight as absent.
	WeightInvalid bool
}

// namespaceAnnotations parses a Namespace object's own metadata.annotations
// using exactly the same rules workloadAnnotations applies to a workload
// object: same keys, same strict 0-9 tier parsing, same strict [0.1,10]
// weight parsing, same "false" exclusion semantics. It discards the
// annotationkeys.VirtualAppGroup return value, which has no namespace-level
// meaning (group membership is a per-workload concept).
func namespaceAnnotations(annotations map[string]string) namespaceDefaults {
	excluded, _, domain, description, tier, hasTier, tierInvalid, weight, hasWeight, weightInvalid := workloadAnnotations(annotations)
	identityMode, identityModeInvalid := parseIdentityMode(annotations[annotationkeys.VirtualAppIdentity])
	return namespaceDefaults{
		Excluded:            excluded,
		TriageExcluded:      TriageExcluded(annotations),
		IdentityMode:        identityMode,
		IdentityModeInvalid: identityModeInvalid,
		Domain:              domain,
		Description:         description,
		Tier:                tier,
		HasTier:             hasTier,
		TierInvalid:         tierInvalid,
		Weight:              weight,
		HasWeight:           hasWeight,
		WeightInvalid:       weightInvalid,
	}
}

// parseIdentityMode maps a raw annotationkeys.VirtualAppIdentity value to a
// canonical IdentityMode (push/virtualapps.go): "" (absent) or the explicit
// "workload" both resolve to the default, returned as the raw value itself,
// since GroupNameFor treats anything other than IdentityModeLabels as
// workload identity, so no separate constant is needed for the zero-value
// case; the explicit "labels" opts into IdentityModeLabels; any other value
// (case-sensitive exact match) falls back to the default with invalid=true,
// so the caller can warn once naming the bad value.
func parseIdentityMode(raw string) (mode string, invalid bool) {
	switch raw {
	case "", IdentityModeWorkload:
		return raw, false
	case IdentityModeLabels:
		return IdentityModeLabels, false
	default:
		return "", true
	}
}

// identityModeDisplay canonicalizes an IdentityMode value for logging: ""
// (the zero value/default) prints as IdentityModeWorkload, so a log line
// naming a namespace's identity mode never shows an empty string.
func identityModeDisplay(mode string) string {
	if mode == IdentityModeLabels {
		return IdentityModeLabels
	}
	return IdentityModeWorkload
}

// applyNamespaceDefaultsToWorkload fills w's Domain/Description/Tier from ns
// wherever w does not declare its own (precedence: workload > namespace >
// absent), and folds the namespace's own exclusion into w.Excluded — once
// ORed in, groupWorkloads needs no further awareness of WHY a workload is
// excluded. w.IdentityMode is set
// unconditionally from ns.IdentityMode (never "filled only if absent" like
// the fields above) since it has no per-workload source at all — every
// workloadFromDeployment/StatefulSet/DaemonSet leaves it at the zero value.
// Called before group aggregation (workloadsSnapshot) so the existing
// uniform-or-empty / min-wins aggregation logic in resolveGroupDomain/
// resolveGroupDescription/resolveGroupTier sees only the already-inherited,
// resolved values — a group whose members all inherit the same namespace
// default resolves that value naturally, with no special-casing.
func applyNamespaceDefaultsToWorkload(w workloadInfo, ns namespaceDefaults) workloadInfo {
	w.Excluded = w.Excluded || ns.Excluded
	w.TriageExcluded = w.TriageExcluded || ns.TriageExcluded
	w.IdentityMode = ns.IdentityMode
	if w.Domain == "" {
		w.Domain = ns.Domain
	}
	if w.Description == "" {
		w.Description = ns.Description
	}
	if !w.HasTier && ns.HasTier {
		w.Tier, w.HasTier = ns.Tier, true
	}
	if !w.HasWeight && ns.HasWeight {
		w.Weight, w.HasWeight = ns.Weight, true
	}
	return w
}

// nsDefaultsStore is the flat, namespace-name-keyed store backing namespace-
// level annotation inheritance onto workloads. Owned by the Pusher and always
// constructed, independent of virtualAppsState (nil when --virtual-apps=off):
// the Namespace informer that fills it runs regardless of mode, even though
// nothing reads it back out until virtual-app synthesis is on.
type nsDefaultsStore struct {
	mu   sync.Mutex
	data map[string]namespaceDefaults
}

func newNsDefaultsStore() *nsDefaultsStore {
	return &nsDefaultsStore{data: make(map[string]namespaceDefaults)}
}

// get returns the stored defaults for ns and whether any have been recorded
// at all (a namespace never seen by the informer, or since removed).
func (s *nsDefaultsStore) get(ns string) (namespaceDefaults, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.data[ns]
	return d, ok
}

func (s *nsDefaultsStore) set(ns string, d namespaceDefaults) {
	s.mu.Lock()
	s.data[ns] = d
	s.mu.Unlock()
}

func (s *nsDefaultsStore) remove(ns string) {
	s.mu.Lock()
	delete(s.data, ns)
	s.mu.Unlock()
}
